package worker

import (
	"sync"
	"testing"
	"time"

	"github.com/aditya10090/go-analytics-engine/internal/aggregator"
)

// TestWorkerProcessesAndSnapshots exercises the running worker's two jobs:
// folding events, and answering a snapshot request over the control channel
// without a data race on its private state.
//
// RequestSnapshot is POINT-IN-TIME by design: the worker's select picks
// between "process an event" and "answer a snapshot", so a snapshot may be
// taken before every buffered event is folded. This is exactly the eventual
// consistency the periodic merge relies on. We therefore poll until the
// snapshot reflects all sent events rather than assuming a single snapshot is
// already complete.
func TestWorkerProcessesAndSnapshots(t *testing.T) {
	w := New(0, 16)
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.Run(done) }()

	for i := 0; i < 10; i++ {
		w.Events() <- aggregator.Event{EventType: "click", UserID: "u1"}
	}
	w.Events() <- aggregator.Event{EventType: "view", UserID: "u2"}

	deadline := time.Now().Add(time.Second)
	var snap aggregator.WorkerSnapshot
	for {
		snap = w.RequestSnapshot()
		if snap.Total == 11 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot never reached 11 events; got %d", snap.Total)
		}
	}
	if snap.EventCounts["click"] != 10 {
		t.Errorf("click = %d, want 10", snap.EventCounts["click"])
	}
	if len(snap.UsersByType["click"]) != 1 {
		t.Errorf("unique click users = %d, want 1", len(snap.UsersByType["click"]))
	}

	close(done)
	wg.Wait()
}

// TestWorkerDrainsOnShutdown checks that events still buffered when done is
// closed are folded in before the worker exits, so FinalSnapshot is complete.
func TestWorkerDrainsOnShutdown(t *testing.T) {
	w := New(0, 100)
	// Fill the buffer WITHOUT a running consumer.
	for i := 0; i < 100; i++ {
		w.Events() <- aggregator.Event{EventType: "e", UserID: "u"}
	}

	done := make(chan struct{})
	close(done) // signal shutdown before Run even starts
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); w.Run(done) }()
	wg.Wait() // Run must drain the 100 buffered events, then return

	if got := w.FinalSnapshot().Total; got != 100 {
		t.Errorf("drained total = %d, want 100 (buffered events lost on shutdown)", got)
	}
}
