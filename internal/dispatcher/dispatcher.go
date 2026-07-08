// Package dispatcher wires the worker pool together. It round-robins incoming
// events onto per-worker channels, runs the periodic merge that publishes a
// read-optimized snapshot, and coordinates graceful shutdown.
//
// Synchronization used here — and NONE of it is a sync.Mutex:
//   - channels: event delivery and snapshot request/reply (the primary
//     happens-before mechanism);
//   - atomic.Uint64: the round-robin cursor (a single fetch-and-add per event);
//   - atomic.Pointer[Snapshot]: lock-free publish/read of the merged snapshot.
package dispatcher

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aditya10090/go-analytics-engine/internal/aggregator"
	"github.com/aditya10090/go-analytics-engine/internal/worker"
)

// ErrNotAccepting is returned by Dispatch once shutdown has begun.
var ErrNotAccepting = errors.New("dispatcher not accepting events")

// Dispatcher owns the worker pool and the merge loop.
type Dispatcher struct {
	workers []*worker.Worker

	next      atomic.Uint64                       // round-robin cursor
	published atomic.Pointer[aggregator.Snapshot] // current /stats view

	mergeInterval time.Duration

	done      chan struct{}  // closed to begin shutdown (stops accepting, drains)
	stopMerge chan struct{}  // closed to stop the merge loop
	mergeDone chan struct{}  // closed when the merge loop has exited
	wg        sync.WaitGroup // tracks worker goroutines
	shutdown  sync.Once
}

// New builds a dispatcher with numWorkers workers, each having a channel buffer
// of the given size, publishing a merged snapshot every mergeInterval.
func New(numWorkers, buffer int, mergeInterval time.Duration) *Dispatcher {
	ws := make([]*worker.Worker, numWorkers)
	for i := range ws {
		ws[i] = worker.New(i, buffer)
	}
	d := &Dispatcher{
		workers:       ws,
		mergeInterval: mergeInterval,
		done:          make(chan struct{}),
		stopMerge:     make(chan struct{}),
		mergeDone:     make(chan struct{}),
	}
	// Publish an empty snapshot so GET /stats works before the first merge.
	empty := aggregator.Merge(numWorkers, nil, time.Now())
	d.published.Store(&empty)
	return d
}

// Start launches the worker goroutines and the merge loop.
func (d *Dispatcher) Start() {
	for _, w := range d.workers {
		d.wg.Add(1)
		go func(w *worker.Worker) {
			defer d.wg.Done()
			w.Run(d.done)
		}(w)
	}
	go d.mergeLoop()
}

// Dispatch routes one event to the next worker in round-robin order.
//
// The send blocks while the chosen worker's buffer is full — natural
// backpressure onto the caller (see DESIGN.md). It never blocks forever: if
// shutdown begins, the done channel unblocks the send and the event is
// rejected with ErrNotAccepting rather than lost silently or panicking on a
// closed channel.
func (d *Dispatcher) Dispatch(ev aggregator.Event) error {
	// Fast reject once shutdown has started.
	select {
	case <-d.done:
		return ErrNotAccepting
	default:
	}

	// Round-robin selection: one atomic fetch-and-add, no lock. Concurrent
	// HTTP handlers each grab a distinct, monotonically increasing ticket.
	i := d.next.Add(1) - 1
	w := d.workers[i%uint64(len(d.workers))]

	select {
	case w.Events() <- ev:
		return nil
	case <-d.done:
		return ErrNotAccepting
	}
}

// Stats returns the current merged snapshot. Lock-free: it loads an atomic
// pointer to an immutable value the merge loop published.
func (d *Dispatcher) Stats() aggregator.Snapshot {
	return *d.published.Load()
}

func (d *Dispatcher) mergeLoop() {
	defer close(d.mergeDone)
	ticker := time.NewTicker(d.mergeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			d.mergeOnce()
		case <-d.stopMerge:
			return
		}
	}
}

// mergeOnce asks every worker for a snapshot and publishes the merged result.
func (d *Dispatcher) mergeOnce() {
	snaps := make([]aggregator.WorkerSnapshot, len(d.workers))
	for i, w := range d.workers {
		snaps[i] = w.RequestSnapshot()
	}
	merged := aggregator.Merge(len(d.workers), snaps, time.Now())
	d.published.Store(&merged)
}

// Shutdown stops accepting events, drains the buffers, flushes a final merged
// snapshot, and returns it. It is safe to call more than once.
//
// Ordering matters and is deliberate:
//  1. Stop the merge loop first and wait for it to exit, so no snapshot request
//     is in flight when workers start shutting down.
//  2. Close done: Dispatch stops accepting and each worker drains its buffer
//     and returns.
//  3. wg.Wait for all workers to exit. This Wait happens-after every worker's
//     final state write, which is what makes the direct FinalSnapshot reads
//     below race-free.
func (d *Dispatcher) Shutdown() aggregator.Snapshot {
	d.shutdown.Do(func() {
		close(d.stopMerge)
		<-d.mergeDone

		close(d.done)
		d.wg.Wait()

		snaps := make([]aggregator.WorkerSnapshot, len(d.workers))
		for i, w := range d.workers {
			snaps[i] = w.FinalSnapshot()
		}
		final := aggregator.Merge(len(d.workers), snaps, time.Now())
		d.published.Store(&final)
	})
	return d.Stats()
}
