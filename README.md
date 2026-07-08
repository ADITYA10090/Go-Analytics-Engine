# Concurrent Analytics Engine (Go)

A small, real HTTP analytics ingestion service built on Go's stdlib
`net/http` — no framework. Events are round-robined by a **dispatcher** onto a
pool of **worker goroutines**, each owning a **buffered channel** and **private
aggregation state**. A periodic **merge** is the single point where per-worker
state is combined into a read-optimized snapshot. There is **no `sync.Mutex`
anywhere** in the codebase; synchronization is done with channels and lock-free
atomics.

Measured throughput on a shared 4-core dev box: **61,389 RPS** sustained
(POST /event), p99 6.5 ms, zero errors — see [BENCHMARKS.md](BENCHMARKS.md).
The design rationale and interview-prep Q&A live in [DESIGN.md](DESIGN.md).

## Architecture

```
                       ┌──────────── worker 0 ── private state ──┐
  POST /event          │  buffered chan ──► goroutine (map++)     │
   ─────────►  Dispatcher ─ round robin ─┼──── worker 1 ── private state ──────────┤
              (atomic cursor)            │  buffered chan ──► goroutine (map++)     │
                       └──────────── worker N ── private state ──┘
                                              │  (control chan: "give me a copy")
                                              ▼
                                       Merge goroutine  ── every merge-interval
                                       unions copies → Snapshot
                                              │  atomic.Pointer store
                                              ▼
  GET /stats ───────────────────────►  load published Snapshot (lock-free)
```

- **Channel-per-worker**, not one shared channel: a slow worker only backs up
  its own queue instead of coupling every worker to the slowest consumer.
- **Private per-worker state**: the hot path is plain Go map writes — no locks,
  no atomics per event.
- **Merge by communication**: the merger receives *immutable deep copies* from
  each worker over a control channel; it never touches a live worker map.
- **Lock-free reads**: `/stats` loads an `atomic.Pointer[Snapshot]`.

## API

### `POST /event`

Body:

```json
{
  "event_type": "click",
  "user_id": "u1",
  "timestamp": 1751990400,
  "properties": {"page": "home"}
}
```

- `event_type` is required (400 otherwise). `timestamp` and `properties` are
  optional. Body is capped at 64 KiB.
- Returns **`202 Accepted`** — ingestion is asynchronous; the event is queued for
  a worker and shows up in `/stats` after the next merge.
- Returns **`503`** once graceful shutdown has begun.

### `GET /stats`

Returns the current merged snapshot:

```json
{
  "generated_at": "2026-07-08T16:07:26Z",
  "workers": 4,
  "total_events": 920672,
  "event_counts": {"click": 920672},
  "unique_users_by_type": {"click": 1},
  "unique_users_overall": 1
}
```

Unique-user counts are the **union** of per-worker ID sets (not a sum), so a
user seen by multiple workers is counted once. See DESIGN §3.

### `GET /healthz`

Returns `200 ok` for readiness/liveness checks.

## Configuration

Every setting is a flag that defaults to an env var, which defaults to a
built-in. Precedence: **flag > env var > default**.

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-addr` | `ADDR` | `:8080` | Listen address |
| `-workers` | `WORKERS` | `NumCPU` (4 here) | Worker goroutines / channels |
| `-buffer` | `BUFFER` | `1024` | Per-worker channel buffer size |
| `-merge-interval` | `MERGE_INTERVAL` | `1s` | Snapshot merge cadence |

**Tuning rationale** (measured, see BENCHMARKS.md):

- `workers = NumCPU` is the robust default. On this workload throughput is
  ingestion-bound, so *fewer* workers were marginally faster — but `NumCPU`
  leaves headroom if per-event work ever grows, without oversubscribing cores.
- `buffer = 1024` is a burst absorber, not a throughput knob (buffer size barely
  affected steady-state rate). It's large enough to ride out a scheduler hiccup,
  small enough to stay cache-friendly. Worst-case memory is bounded by
  `workers × buffer × sizeof(Event)`.
- `merge-interval = 1s` bounds how stale `/stats` can be. Shorter = fresher but
  more copying; 1s is the usual analytics freshness/overhead trade-off.

## Run

```bash
go run ./cmd/analytics                       # defaults (:8080, workers=NumCPU)
go run ./cmd/analytics -workers 8 -buffer 2048 -addr :9000
WORKERS=2 BUFFER=512 go run ./cmd/analytics  # via env

# try it
curl -X POST localhost:8080/event \
  -d '{"event_type":"click","user_id":"u1","properties":{"page":"home"}}'
curl -s localhost:8080/stats | jq
```

`Ctrl-C` (SIGINT/SIGTERM) triggers graceful shutdown: stop accepting new events,
drain worker channels, flush the final aggregation, then exit — the drained
total is logged.

## Test

```bash
go test ./...            # all unit + integration tests
go test -race ./...      # race detector (the correctness proof for the model)
go vet ./...
```

What's covered:

- `internal/aggregator` — aggregation correctness, deep-copy isolation, and the
  **union-not-sum** unique-user property.
- `internal/dispatcher` — **round-robin fairness** (sequential *and* concurrent:
  N×K dispatches land exactly K per worker), shutdown rejection, idempotent
  shutdown, and end-to-end merge publication.
- `internal/worker` — event folding, point-in-time snapshotting, drain-on-shutdown.
- `internal/httpapi` — **integration test**: real HTTP server hit by 20
  concurrent clients × 500 requests, asserting `/stats` totals and unique counts
  are exactly correct.

## Benchmark

```bash
./loadtest/run.sh                 # sweep workers & buffer, print RPS per setting
DURATION=15s CONC=100 ./loadtest/run.sh
```

`run.sh` uses [`hey`](https://github.com/rakyll/hey) if installed, otherwise the
bundled self-contained Go generator at `loadtest/loadgen`. Full results,
including the sweeps and the bottleneck analysis, are in
[BENCHMARKS.md](BENCHMARKS.md).

## Layout

```
cmd/analytics          main: flags/env, HTTP server, signal handling, shutdown
internal/aggregator    Event, LocalState (hot path), Snapshot, Merge
internal/worker        Worker goroutine: owns a channel + private state
internal/dispatcher    round-robin dispatch, merge loop, graceful shutdown
internal/httpapi       net/http handlers (/event, /stats, /healthz)
loadtest/loadgen       self-contained Go load generator
loadtest/run.sh        worker/buffer sweep driver
DESIGN.md              why each choice was made (interview prep)
BENCHMARKS.md          real measured numbers + bottleneck analysis
```
