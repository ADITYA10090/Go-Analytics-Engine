// Package worker implements the unit of concurrency in the engine.
//
// A Worker owns one buffered channel and one private LocalState. A single
// goroutine (Run) drains the channel and folds each event into the state, so
// the state is never shared and never needs a lock. The only way to read a
// worker's state from outside is to ask for an immutable copy over the control
// channel — "share memory by communicating", not by locking.
package worker

import (
	"github.com/aditya10090/go-analytics-engine/internal/aggregator"
)

// snapshotRequest asks the worker to copy its state and send it back on reply.
type snapshotRequest struct {
	reply chan aggregator.WorkerSnapshot
}

// Worker drains its own buffered channel and aggregates into private state.
type Worker struct {
	ID      int
	events  chan aggregator.Event
	control chan snapshotRequest
	state   *aggregator.LocalState
}

// New builds a worker with an events channel of the given buffer size.
func New(id, buffer int) *Worker {
	return &Worker{
		ID:      id,
		events:  make(chan aggregator.Event, buffer),
		control: make(chan snapshotRequest),
		state:   aggregator.NewLocalState(),
	}
}

// Events returns the send side of the worker's queue. The dispatcher sends
// events here; it is never closed (shutdown is signalled out-of-band via the
// done channel passed to Run), so senders can never panic on a closed channel.
func (w *Worker) Events() chan<- aggregator.Event { return w.events }

// Run is the worker goroutine. It processes events and answers snapshot
// requests until done is closed, then drains whatever is still buffered and
// returns. Exactly this goroutine touches w.state.
func (w *Worker) Run(done <-chan struct{}) {
	for {
		select {
		case ev := <-w.events:
			w.state.Add(ev)
		case req := <-w.control:
			req.reply <- w.state.Snapshot()
		case <-done:
			w.drain()
			return
		}
	}
}

// drain folds any events still sitting in the buffer, then returns. Called once
// during shutdown after done is closed. By the time this runs the dispatcher
// guarantees no new sends will be committed (see dispatcher.Shutdown), so an
// empty buffer means the worker is truly finished.
func (w *Worker) drain() {
	for {
		select {
		case ev := <-w.events:
			w.state.Add(ev)
		default:
			return
		}
	}
}

// RequestSnapshot asks the running worker for an immutable copy of its state.
// It blocks until the worker services the request on its own goroutine, so the
// copy is made with no concurrent writer. Called only by the merge goroutine
// while the worker is running.
func (w *Worker) RequestSnapshot() aggregator.WorkerSnapshot {
	reply := make(chan aggregator.WorkerSnapshot)
	w.control <- snapshotRequest{reply: reply}
	return <-reply
}

// FinalSnapshot copies the state directly, WITHOUT going through the control
// channel. It is only safe to call after Run has returned: the goroutine that
// owned the state has stopped, and the caller established a happens-before edge
// (a sync.WaitGroup.Wait in dispatcher.Shutdown) that makes every prior write
// visible. Calling it while Run is active is a data race.
func (w *Worker) FinalSnapshot() aggregator.WorkerSnapshot {
	return w.state.Snapshot()
}
