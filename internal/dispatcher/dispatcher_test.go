package dispatcher

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aditya10090/go-analytics-engine/internal/aggregator"
)

// TestRoundRobinFairnessSequential checks that N*K sequential dispatches land
// exactly K on each of the N workers — the defining property of round-robin.
func TestRoundRobinFairnessSequential(t *testing.T) {
	const workers, k = 4, 250
	d := New(workers, 1024, time.Hour) // long merge interval: irrelevant here
	d.Start()

	for i := 0; i < workers*k; i++ {
		if err := d.Dispatch(aggregator.Event{EventType: "e", UserID: fmt.Sprintf("u%d", i)}); err != nil {
			t.Fatalf("dispatch %d: %v", i, err)
		}
	}
	d.Shutdown()

	for i, w := range d.workers {
		got := w.FinalSnapshot().Total
		if got != k {
			t.Errorf("worker %d received %d events, want %d (unfair distribution)", i, got, k)
		}
	}
}

// TestRoundRobinFairnessConcurrent checks the same fairness property under
// concurrent dispatch. Because the round-robin cursor is a single atomic
// fetch-and-add, N*K dispatches from many goroutines still consume tickets
// 0..N*K-1 exactly once, so each worker must end up with exactly K.
func TestRoundRobinFairnessConcurrent(t *testing.T) {
	const workers, perG, goroutines = 8, 500, 16
	d := New(workers, 2048, time.Hour)
	d.Start()

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				if err := d.Dispatch(aggregator.Event{EventType: "e", UserID: fmt.Sprintf("u%d-%d", g, i)}); err != nil {
					t.Errorf("dispatch: %v", err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	d.Shutdown()

	total := goroutines * perG
	want := int64(total / workers)
	for i, w := range d.workers {
		got := w.FinalSnapshot().Total
		if got != want {
			t.Errorf("worker %d received %d events, want %d", i, got, want)
		}
	}
}

// TestDispatchRejectsAfterShutdown verifies the backpressure/shutdown contract:
// once shutdown begins, Dispatch returns ErrNotAccepting instead of sending on
// (and potentially panicking on) a defunct worker.
func TestDispatchRejectsAfterShutdown(t *testing.T) {
	d := New(2, 8, time.Hour)
	d.Start()
	d.Shutdown()

	if err := d.Dispatch(aggregator.Event{EventType: "e"}); err != ErrNotAccepting {
		t.Fatalf("Dispatch after shutdown = %v, want ErrNotAccepting", err)
	}
}

// TestShutdownIsIdempotent makes sure a double shutdown does not panic or
// deadlock (the HTTP path and a signal handler could both trigger it).
func TestShutdownIsIdempotent(t *testing.T) {
	d := New(2, 8, time.Hour)
	d.Start()
	a := d.Shutdown()
	b := d.Shutdown()
	if a.TotalEvents != b.TotalEvents {
		t.Errorf("idempotent shutdown returned different snapshots: %d vs %d", a.TotalEvents, b.TotalEvents)
	}
}

// TestMergePublishesSnapshot checks that the periodic merge actually publishes
// aggregated results to /stats readers while the engine runs.
func TestMergePublishesSnapshot(t *testing.T) {
	d := New(4, 1024, 10*time.Millisecond)
	d.Start()
	defer d.Shutdown()

	const n = 1000
	for i := 0; i < n; i++ {
		if err := d.Dispatch(aggregator.Event{EventType: "click", UserID: fmt.Sprintf("u%d", i%50)}); err != nil {
			t.Fatalf("dispatch: %v", err)
		}
	}

	// Poll the published snapshot until the merge catches up.
	deadline := time.Now().Add(2 * time.Second)
	for {
		s := d.Stats()
		if s.TotalEvents == n {
			if s.EventCounts["click"] != n {
				t.Errorf("click count = %d, want %d", s.EventCounts["click"], n)
			}
			if s.UniqueUsersOverall != 50 {
				t.Errorf("unique users = %d, want 50", s.UniqueUsersOverall)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("merge did not reach %d events; last snapshot total = %d", n, s.TotalEvents)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
