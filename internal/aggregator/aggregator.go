// Package aggregator holds the analytics data model and the two kinds of
// aggregation state the engine uses:
//
//   - LocalState is mutable, private to a single worker goroutine, and updated
//     on the hot path. Because exactly one goroutine ever touches a given
//     LocalState, its updates need no synchronization.
//   - Snapshot is the immutable, read-optimized view served by GET /stats. It
//     is produced by Merge, which unions the per-worker snapshots. Merge is the
//     one and only place per-worker state is combined.
package aggregator

import "time"

// Event is a single analytics event as ingested by POST /event.
//
// Properties is an opaque bag carried along with the event; the engine does not
// aggregate on it today, but it is decoded and validated so the request path
// mirrors a real ingestion endpoint.
type Event struct {
	EventType  string         `json:"event_type"`
	UserID     string         `json:"user_id"`
	Timestamp  int64          `json:"timestamp"`
	Properties map[string]any `json:"properties,omitempty"`
}

// LocalState is one worker's private aggregation state.
//
// INVARIANT: a LocalState is owned by exactly one goroutine for its entire
// lifetime. All mutation (Add) and copying (Snapshot) happen inside that
// goroutine. That single-ownership is the reason the hot path needs no mutex —
// there is simply no sharing to protect.
type LocalState struct {
	total       int64
	eventCounts map[string]int64
	// usersByType maps event_type -> set of user IDs seen for that type.
	// We keep the full ID sets (not just counts) because unique-user counts
	// cannot be summed across workers: the same user can be round-robined onto
	// different workers, so the true unique count is the size of the UNION of
	// the per-worker sets. Merge does that union.
	usersByType map[string]map[string]struct{}
}

// NewLocalState returns an empty LocalState ready for a single worker to own.
func NewLocalState() *LocalState {
	return &LocalState{
		eventCounts: make(map[string]int64),
		usersByType: make(map[string]map[string]struct{}),
	}
}

// Add folds one event into the state. Called only by the owning worker.
func (s *LocalState) Add(ev Event) {
	s.total++
	s.eventCounts[ev.EventType]++
	if ev.UserID == "" {
		return
	}
	set := s.usersByType[ev.EventType]
	if set == nil {
		set = make(map[string]struct{})
		s.usersByType[ev.EventType] = set
	}
	set[ev.UserID] = struct{}{}
}

// WorkerSnapshot is an immutable, deep copy of one worker's state at a point in
// time. The worker produces it inside its own goroutine and hands ownership to
// the merger over a channel; the merger then reads it without any locking,
// because it now owns a copy the worker will never touch again.
type WorkerSnapshot struct {
	Total       int64
	EventCounts map[string]int64
	UsersByType map[string]map[string]struct{}
}

// Snapshot deep-copies the local state into an immutable WorkerSnapshot.
// Called only by the owning worker (either on a control-channel request during
// steady state, or directly after the worker goroutine has exited during
// shutdown — see worker.Worker).
func (s *LocalState) Snapshot() WorkerSnapshot {
	ec := make(map[string]int64, len(s.eventCounts))
	for k, v := range s.eventCounts {
		ec[k] = v
	}
	ubt := make(map[string]map[string]struct{}, len(s.usersByType))
	for t, set := range s.usersByType {
		cp := make(map[string]struct{}, len(set))
		for u := range set {
			cp[u] = struct{}{}
		}
		ubt[t] = cp
	}
	return WorkerSnapshot{Total: s.total, EventCounts: ec, UsersByType: ubt}
}

// Snapshot is the merged, read-optimized view returned by GET /stats.
//
// It stores only counts — never the raw ID sets — so it is small and cheap to
// publish (a single atomic pointer swap) and cheap for readers to serialize.
type Snapshot struct {
	GeneratedAt        time.Time        `json:"generated_at"`
	Workers            int              `json:"workers"`
	TotalEvents        int64            `json:"total_events"`
	EventCounts        map[string]int64 `json:"event_counts"`
	UniqueUsersByType  map[string]int64 `json:"unique_users_by_type"`
	UniqueUsersOverall int64            `json:"unique_users_overall"`
}

// Merge unions per-worker snapshots into a single read-optimized Snapshot.
//
// This is the ONLY place in the engine where per-worker state is combined. It
// runs in the single merge goroutine and reads only the immutable snapshots it
// was handed, so it needs no locks. Counts are summed; unique-user sets are
// unioned (not summed) and then reduced to cardinalities.
func Merge(workers int, snaps []WorkerSnapshot, now time.Time) Snapshot {
	counts := make(map[string]int64)
	usersByType := make(map[string]map[string]struct{})
	overall := make(map[string]struct{})
	var total int64

	for _, snap := range snaps {
		total += snap.Total
		for t, c := range snap.EventCounts {
			counts[t] += c
		}
		for t, set := range snap.UsersByType {
			union := usersByType[t]
			if union == nil {
				union = make(map[string]struct{})
				usersByType[t] = union
			}
			for u := range set {
				union[u] = struct{}{}
				overall[u] = struct{}{}
			}
		}
	}

	uniqByType := make(map[string]int64, len(usersByType))
	for t, set := range usersByType {
		uniqByType[t] = int64(len(set))
	}

	return Snapshot{
		GeneratedAt:        now,
		Workers:            workers,
		TotalEvents:        total,
		EventCounts:        counts,
		UniqueUsersByType:  uniqByType,
		UniqueUsersOverall: int64(len(overall)),
	}
}
