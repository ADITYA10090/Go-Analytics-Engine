package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/aditya10090/go-analytics-engine/internal/aggregator"
	"github.com/aditya10090/go-analytics-engine/internal/dispatcher"
)

// TestIntegrationConcurrentLoad spins up the real HTTP server, hammers
// POST /event from many goroutines with a known event/user distribution, then
// asserts GET /stats reports the correct aggregation. This is the end-to-end
// check across the whole pipeline: handler -> dispatcher -> workers -> merge.
func TestIntegrationConcurrentLoad(t *testing.T) {
	d := dispatcher.New(4, 1024, 10*time.Millisecond)
	d.Start()
	srv := httptest.NewServer(New(d).Handler())
	t.Cleanup(func() {
		srv.Close()
		d.Shutdown()
	})

	const (
		goroutines   = 20
		perGoroutine = 500
		uniqueUsers  = 100
	)
	total := goroutines * perGoroutine

	client := srv.Client()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				// Two event types; user id cycles through uniqueUsers values.
				etype := "click"
				if (g+i)%2 == 0 {
					etype = "view"
				}
				body, _ := json.Marshal(aggregator.Event{
					EventType: etype,
					UserID:    fmt.Sprintf("user-%d", (g*perGoroutine+i)%uniqueUsers),
					Timestamp: time.Now().Unix(),
				})
				resp, err := client.Post(srv.URL+"/event", "application/json", bytes.NewReader(body))
				if err != nil {
					t.Errorf("post: %v", err)
					return
				}
				resp.Body.Close()
				if resp.StatusCode != http.StatusAccepted {
					t.Errorf("status = %d, want 202", resp.StatusCode)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	// Poll /stats until the merge reflects every event.
	deadline := time.Now().Add(3 * time.Second)
	var snap aggregator.Snapshot
	for {
		snap = getStats(t, client, srv.URL)
		if snap.TotalEvents == int64(total) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stats never reached %d events; got %d", total, snap.TotalEvents)
		}
		time.Sleep(5 * time.Millisecond)
	}

	clicks := snap.EventCounts["click"]
	views := snap.EventCounts["view"]
	if clicks+views != int64(total) {
		t.Errorf("click+view = %d, want %d", clicks+views, total)
	}
	if snap.UniqueUsersOverall != uniqueUsers {
		t.Errorf("unique users = %d, want %d", snap.UniqueUsersOverall, uniqueUsers)
	}
}

func TestEventValidation(t *testing.T) {
	d := dispatcher.New(2, 16, time.Hour)
	d.Start()
	srv := httptest.NewServer(New(d).Handler())
	t.Cleanup(func() { srv.Close(); d.Shutdown() })
	client := srv.Client()

	cases := []struct {
		name, method, body string
		want               int
	}{
		{"valid", http.MethodPost, `{"event_type":"click","user_id":"u1"}`, http.StatusAccepted},
		{"missing event_type", http.MethodPost, `{"user_id":"u1"}`, http.StatusBadRequest},
		{"malformed json", http.MethodPost, `{not json`, http.StatusBadRequest},
		{"wrong method", http.MethodGet, ``, http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, srv.URL+"/event", bytes.NewBufferString(tc.body))
			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

func TestStatsRejectsPost(t *testing.T) {
	d := dispatcher.New(2, 16, time.Hour)
	d.Start()
	srv := httptest.NewServer(New(d).Handler())
	t.Cleanup(func() { srv.Close(); d.Shutdown() })

	resp, err := srv.Client().Post(srv.URL+"/stats", "application/json", nil)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", resp.StatusCode)
	}
}

func getStats(t *testing.T, client *http.Client, base string) aggregator.Snapshot {
	t.Helper()
	resp, err := client.Get(base + "/stats")
	if err != nil {
		t.Fatalf("get stats: %v", err)
	}
	defer resp.Body.Close()
	var snap aggregator.Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode stats: %v", err)
	}
	return snap
}
