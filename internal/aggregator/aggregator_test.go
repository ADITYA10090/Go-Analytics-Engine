package aggregator

import (
	"testing"
	"time"
)

func TestLocalStateAdd(t *testing.T) {
	s := NewLocalState()
	s.Add(Event{EventType: "click", UserID: "u1"})
	s.Add(Event{EventType: "click", UserID: "u2"})
	s.Add(Event{EventType: "click", UserID: "u1"}) // repeat user
	s.Add(Event{EventType: "view", UserID: "u1"})

	snap := s.Snapshot()
	if snap.Total != 4 {
		t.Fatalf("total = %d, want 4", snap.Total)
	}
	if snap.EventCounts["click"] != 3 {
		t.Errorf("click count = %d, want 3", snap.EventCounts["click"])
	}
	if snap.EventCounts["view"] != 1 {
		t.Errorf("view count = %d, want 1", snap.EventCounts["view"])
	}
	if got := len(snap.UsersByType["click"]); got != 2 {
		t.Errorf("unique click users = %d, want 2", got)
	}
}

// TestSnapshotIsDeepCopy guards the core safety property: a snapshot must not
// alias the worker's live maps, or the merge goroutine and the worker would
// share mutable state.
func TestSnapshotIsDeepCopy(t *testing.T) {
	s := NewLocalState()
	s.Add(Event{EventType: "click", UserID: "u1"})

	snap := s.Snapshot()
	// Mutate the live state after snapshotting.
	s.Add(Event{EventType: "click", UserID: "u2"})

	if snap.EventCounts["click"] != 1 {
		t.Errorf("snapshot count changed to %d; snapshot aliases live map", snap.EventCounts["click"])
	}
	if len(snap.UsersByType["click"]) != 1 {
		t.Errorf("snapshot user set changed to %d; snapshot aliases live map", len(snap.UsersByType["click"]))
	}
}

// TestMergeUnionsUniqueUsers is the aggregation-correctness test that matters
// most: unique users cannot be summed across workers. The same user landing on
// two different workers must count once.
func TestMergeUnionsUniqueUsers(t *testing.T) {
	// Worker A saw u1, u2 clicking.
	a := NewLocalState()
	a.Add(Event{EventType: "click", UserID: "u1"})
	a.Add(Event{EventType: "click", UserID: "u2"})
	// Worker B saw u2, u3 clicking (u2 overlaps with A).
	b := NewLocalState()
	b.Add(Event{EventType: "click", UserID: "u2"})
	b.Add(Event{EventType: "click", UserID: "u3"})
	b.Add(Event{EventType: "view", UserID: "u1"})

	merged := Merge(2, []WorkerSnapshot{a.Snapshot(), b.Snapshot()}, time.Now())

	if merged.TotalEvents != 5 {
		t.Errorf("total = %d, want 5", merged.TotalEvents)
	}
	if merged.EventCounts["click"] != 4 {
		t.Errorf("click count = %d, want 4 (summed)", merged.EventCounts["click"])
	}
	// u1, u2, u3 clicked => 3 unique, NOT 4 (which is what summing would give).
	if merged.UniqueUsersByType["click"] != 3 {
		t.Errorf("unique click users = %d, want 3 (union, not sum)", merged.UniqueUsersByType["click"])
	}
	// u1, u2, u3 across all types => 3 overall.
	if merged.UniqueUsersOverall != 3 {
		t.Errorf("unique users overall = %d, want 3", merged.UniqueUsersOverall)
	}
	if merged.Workers != 2 {
		t.Errorf("workers = %d, want 2", merged.Workers)
	}
}

func TestMergeEmpty(t *testing.T) {
	merged := Merge(4, nil, time.Now())
	if merged.TotalEvents != 0 {
		t.Errorf("total = %d, want 0", merged.TotalEvents)
	}
	if merged.Workers != 4 {
		t.Errorf("workers = %d, want 4", merged.Workers)
	}
	if merged.EventCounts == nil {
		t.Error("EventCounts should be a non-nil (empty) map for clean JSON output")
	}
}

func TestAddEmptyUserID(t *testing.T) {
	s := NewLocalState()
	s.Add(Event{EventType: "ping"}) // no user id
	snap := s.Snapshot()
	if snap.Total != 1 || snap.EventCounts["ping"] != 1 {
		t.Fatalf("event without user id should still count: %+v", snap)
	}
	if len(snap.UsersByType["ping"]) != 0 {
		t.Errorf("empty user id should not be tracked as a unique user")
	}
}
