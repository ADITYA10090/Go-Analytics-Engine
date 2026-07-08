# DESIGN

This document explains *why* the engine is built the way it is. It is written
for interview defense: every claim below maps to code you can point at.

## 1. The concurrency model in one paragraph

Incoming events are round-robined by a **dispatcher** onto **N worker
goroutines**, each with its **own buffered channel** (channel-per-worker, not a
shared channel). Each worker folds events into **private aggregation state** that
no other goroutine ever touches, so the hot path has no lock. Once per
`merge-interval` a single **merge goroutine** asks each worker — over a control
channel — for an immutable copy of its state, unions those copies into one
read-optimized `Snapshot`, and publishes it via an atomic pointer. `GET /stats`
loads that pointer. **The merge is the only place per-worker state is combined,
and it does so by receiving copies over channels, never by sharing memory.**

### What synchronizes what (and note: no `sync.Mutex` anywhere)

| Mechanism | Where | Purpose |
|---|---|---|
| Buffered `chan Event` (one per worker) | `worker.events` | Deliver events; the buffer absorbs bursts |
| `atomic.Uint64` fetch-and-add | `dispatcher.next` | Round-robin cursor across concurrent HTTP handlers |
| Control `chan snapshotRequest` + reply chan | `worker.control` | Merge goroutine pulls an immutable state copy |
| `atomic.Pointer[Snapshot]` | `dispatcher.published` | Lock-free publish/read of the merged snapshot |
| `chan struct{}` (`done`, `stopMerge`, `mergeDone`) | shutdown | Signal draining and ordered teardown |
| `sync.WaitGroup` | `dispatcher.wg` | Wait for workers to exit; its `Wait` gives the happens-before that makes the final direct state read safe |

There is **zero `sync.Mutex`** in the codebase — not just off the request path.
`grep -rn "sync.Mutex" .` returns nothing. The atomics used (`atomic.Uint64`,
`atomic.Pointer`) are lock-free single-word operations, not locks around a
critical section.

## 2. Why round-robin over least-loaded or consistent-hashing dispatch?

**Round-robin** is a single atomic increment (`next.Add(1)`) followed by a
modulo. It needs no knowledge of worker queue depths, so it introduces no shared
observable state beyond one counter and can't become a contention point the way
a shared "find the least-loaded worker" scan would. For a workload where every
event costs about the same to process (a few map writes), even distribution *is*
optimal distribution — there is nothing for a smarter policy to exploit.

- **vs. least-loaded:** least-loaded has to read every worker's queue length on
  each dispatch (more shared reads, a race or a lock), and only pays off when
  per-event cost varies wildly. Ours doesn't.
- **vs. consistent hashing (e.g. hash by `user_id`):** hashing would pin each
  user to one worker, which *removes* the need to union user-sets at merge time
  (a worker would own a user's whole history). That's a real, tempting
  alternative. We rejected it because (a) it reintroduces hot-partition risk —
  one whale user or a skewed hash stalls one worker while others idle, and (b)
  round-robin keeps the dispatcher completely stateless per event. We pay for
  that choice with set-union at merge time (see §"unique users"), which is cheap
  at our snapshot cadence.

**Round-robin's failure mode:** because worker *i* always gets events
`i, i+N, i+2N, …` regardless of how busy it is, a single **slow worker** (e.g.
descheduled by the OS, or on a slow core) backs up *its own* queue while the
others drain fine. Traffic destined for that worker piles up behind it.

**How buffered channels mitigate it:** the per-worker buffer is a shock
absorber. A worker that stalls briefly doesn't immediately block the dispatcher;
its buffer soaks up the events aimed at it until it catches up. Only when that
one worker's buffer *fills* does back-pressure reach the dispatcher — and even
then it's isolated to the fraction of traffic hashed to that worker, not the
whole pipeline (that isolation is precisely why we use a channel *per worker*
instead of one shared channel: a shared channel would couple all workers to the
slowest consumer). The buffer trades memory for tolerance of short stalls; it
does **not** fix a *permanently* slow worker (see §3).

## 3. Why per-worker local state instead of a shared map behind a mutex?

A shared `map[string]int64` behind a `sync.Mutex` would put a lock acquisition on
**every event** at 60k+ events/sec — that lock becomes the single most contended
object in the program, and all worker parallelism collapses onto it. A
`sync.Map` or sharded-lock map helps but still pays atomic/lock costs per write
and complicates unique-user set semantics.

Instead each worker owns a plain `map` it alone reads and writes. No atomics, no
locks, cache-friendly — just Go map writes. This is the "don't communicate by
sharing memory; share memory by communicating" principle applied literally.

**What exactly makes the merge race-free?** The merge goroutine never reads a
worker's live map. It sends a `snapshotRequest` on the worker's control channel;
the worker — inside its own single-threaded `select` loop — makes a **deep copy**
of its maps (`LocalState.Snapshot()`) and sends that copy back on a reply
channel. Two guarantees stack up:

1. **No concurrent access:** the copy is made *by the worker goroutine itself*,
   interleaved with (never concurrent to) its event processing, because a single
   goroutine can only do one `select` case at a time.
2. **Ownership transfer:** the channel send/receive establishes a happens-before
   edge (Go memory model), and the value sent is a fresh deep copy the worker
   will never mutate again. The merger becomes the sole owner of that copy.

So there is no shared mutable state between the worker and the merger at any
instant — nothing to lock. `TestSnapshotIsDeepCopy` guards guarantee (2) by
mutating the live state after snapshotting and asserting the snapshot is
unchanged.

During **shutdown** we take a shortcut that is still race-free: after
`wg.Wait()` returns, every worker goroutine has exited, so nobody can be writing
the state. `Wait()` happens-after every worker's final write, which lets
`Shutdown` read each `LocalState` directly (`FinalSnapshot`) without the control
channel. This is documented on `worker.FinalSnapshot`.

### Unique users: why we union sets instead of summing counts

Round-robin means the *same* `user_id` can be handled by different workers over
time. You therefore **cannot** compute distinct-user counts by summing each
worker's count — that double-counts users seen by more than one worker. Each
worker keeps the actual **set** of user IDs it has seen (per event type); the
merge **unions** those sets and reports the cardinality. `TestMergeUnionsUnique
Users` pins this: two workers that both saw `u2` yield 3 unique users, not 4.

The cost is that per-worker snapshots carry ID sets, so a merge is O(total
distinct users). At our 1-second cadence that's fine. The production answer is a
probabilistic sketch (HyperLogLog) — O(1) memory per counter, mergeable, ~2%
error — noted in §5.

## 4. Backpressure: block, drop, or reject when a worker's channel is full?

We **block** (apply back-pressure), with a shutdown-aware escape hatch.
`Dispatch` does:

```go
select {
case w.Events() <- ev:   // blocks here while the buffer is full
    return nil
case <-d.done:           // …unless shutdown starts, then reject
    return ErrNotAccepting
}
```

**Why block?** For an analytics pipeline, silently **dropping** events corrupts
the very counts the system exists to produce, and **rejecting** (503) just pushes
the retry decision onto every client for a condition that is usually a
momentary burst. Blocking propagates back-pressure naturally up the stack: a
full buffer slows the HTTP handler, which slows the client's connection, which is
exactly the signal an overloaded system should send. No event is lost and no
client has to implement retry logic for transient pressure.

**The trade-off we accept:** blocking couples request latency to worker
progress. If a worker is *permanently* wedged (not just briefly slow), its share
of requests will eventually block indefinitely — round-robin has no way to route
around it. We consider that acceptable because (a) our workers can't block on I/O
(pure in-memory map writes), so a permanent stall implies a bug or a dead core,
and (b) the `done` channel guarantees blocked senders always unblock at shutdown
rather than hanging the process. The alternative policies are each better for a
*different* goal, and the choice is a one-line change in `Dispatch`:

| Policy | Pick it when | Cost |
|---|---|---|
| **Block** (ours) | Losing events is unacceptable; bursts are transient | Latency tracks worker progress |
| Drop (`select { case ch<-ev: default: }`) | Approximate metrics you can afford to lose; must never add latency | Silent undercount under load |
| Reject (503) | Clients can retry/shed and you want to shed load explicitly | Every client needs retry logic |

## 5. Scaling past a single process, and what's missing vs. production

**Horizontal scale.** This process is one shard. To scale out:

- Put **multiple instances** behind an L4/L7 **load balancer**. Because ingestion
  is stateless per event, any instance can accept any event.
- Aggregation then becomes a two-level version of what we already do: each
  instance produces a `Snapshot`, and a **roll-up tier** merges snapshots across
  instances — the exact same `aggregator.Merge` logic, one level up. Sum the
  counts; **union** the unique-user sketches (this is where HLL earns its keep —
  you can't union exact sets across dozens of instances cheaply, but you can
  union HLL registers).
- If you need per-user consistency (sessionization, ordered per-user state),
  shard the *load balancer* by `user_id` (consistent hashing at the LB, not
  inside the process) so a user's events always reach the same instance.

**What's deliberately missing compared to a production pipeline:**

1. **Durable, at-least-once ingestion.** Today an event lives only in a worker's
   in-memory buffer until merged. A crash loses un-merged events, and the
   shutdown drain is **at-most-once** (see below). Production fronts ingestion
   with a durable log — **Kafka / Kinesis / Pub-Sub** — so events survive
   consumer restarts and can be replayed. Workers become consumer-group members;
   offsets give at-least-once with idempotent aggregation.
2. **Durable storage / persistence.** The snapshot is RAM only; a restart resets
   all counts. Production writes snapshots to a time-series or OLAP store
   (ClickHouse, Druid, Postgres+rollups) and serves history, not just "since
   boot".
3. **Approximate distinct counts at scale (HyperLogLog).** Exact ID sets don't
   fit in memory at billions of users; HLL trades ~2% error for fixed memory and
   cheap cross-shard union.
4. **Windowing / time-bucketing.** We keep lifetime totals. Real analytics needs
   tumbling/sliding windows (per-minute, per-hour) and watermarks for late data.
5. **Observability & back-pressure signals.** Per-worker queue-depth metrics,
   dropped/blocked counters, and dispatch latency histograms — so operators can
   see the slow-worker failure mode from §2 before it hurts.
6. **Schema / validation / auth.** Real endpoints validate against a schema,
   authenticate producers, and rate-limit.

### Honest note on shutdown semantics (at-most-once)

Graceful shutdown closes `done`, which (a) makes `Dispatch` reject new events and
(b) tells each worker to drain its buffer and exit; `Shutdown` then reads the
fully-drained final state. This flushes everything already in the buffers. The
one imperfection: an event whose `select` commits to the channel send in the
same instant a worker's drain loop hits its final empty check could be left in
the buffer and not counted. The window is sub-microsecond and only exists during
shutdown, and the correct fix is durable ingestion (item 1) rather than more
in-process coordination — so we document it as **at-most-once during shutdown**
rather than pretend it's exactly-once. The 920,957-event benchmark below drained
with `total_events=920957` — zero loss in practice — but the guarantee is stated
honestly.
