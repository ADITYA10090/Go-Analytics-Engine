# BENCHMARKS

All numbers below are real output from `hey` against `POST /event` on the dev
machine described. Nothing is rounded up or invented. Where a run *disproved* a
hypothesis (e.g. "more workers = more throughput"), that's recorded too, because
the negative result is the interesting one.

## Machine

- 4 vCPU, 15 GiB RAM, Linux 6.18.5, Go 1.24.7
- **The load generator (`hey`) runs on the same 4 cores as the server.** They
  compete for CPU, so these figures are a *lower bound* on server capacity — a
  dedicated load box would push higher. Reproducing on separate hosts is the
  honest way to get the server's true ceiling.
- Payload (77 bytes): `{"event_type":"click","user_id":"u1","timestamp":0,"properties":{"page":"home"}}`

## Headline result (satisfies the 50k+ RPS claim)

`workers=4 buffer=1024`, `hey -z 15s -c 100`:

```
Requests/sec:   61389.6085
Total requests: 920,957   (all HTTP 202, 0 errors)
Latency:        p50 1.3ms  p90 3.5ms  p95 4.4ms  p99 6.5ms   slowest 39ms
Status codes:   [202] 920957 responses
```

**61,389 RPS sustained**, above the 50k target, with a p99 of 6.5 ms and zero
errors. On graceful shutdown the server logged:

```
drained: total_events=920957 unique_users=1
```

That number **exactly equals** the 920,957 accepted responses — every accepted
event was aggregated and flushed, i.e. **zero event loss** end-to-end. (The
`/stats` snapshot taken mid-run read 920,672 because it reflects the last
1-second merge, not the in-flight buffer; the shutdown drain flushed the
remainder.)

## Worker-count sweep — the key finding

`buffer=1024`, `hey -z 10s -c 50`, one worker count per row:

| workers | Requests/sec | events ingested (10s) |
|--------:|-------------:|----------------------:|
| 1       | 62,445.78    | 623,869               |
| 2       | 60,656.28    | 606,051               |
| 4       | 60,861.63    | 607,959               |
| 8       | 59,991.74    | 599,484               |
| 16      | 59,153.74    | 591,056               |

**Throughput is flat — even slightly *decreasing* — as workers increase.** This
is the most important result in the repo, and it's a *positive* thing to
understand, not a failure:

- The bottleneck is **ingestion** (accept → JSON decode → dispatch), not
  **aggregation**. A worker's job is a handful of map writes, which is orders of
  magnitude cheaper than parsing an HTTP request. A *single* worker already
  drains its channel faster than `net/http` can fill it, so adding workers gives
  nothing to do.
- More workers slightly *hurts*: more goroutines contending for the same 4
  cores (shared with `hey`), more channels, and the round-robin cursor spreading
  cache lines wider. Hence the gentle downward slope.
- **Interview takeaway:** a worker pool pays off when per-item work is expensive
  or blocks (CPU-heavy transforms, I/O, downstream calls). For trivial in-memory
  aggregation you become ingestion-bound almost immediately — and the right move
  is to *measure* that rather than assume linear scaling. The architecture still
  matters: it's what keeps aggregation off the critical path so we *stay*
  ingestion-bound.

Default `workers = NumCPU` (4 here) is chosen not because it maximized
throughput (1 worker did) but because it's the robust default: it keeps
headroom for the aggregation side if per-event work ever grows, without
oversubscribing cores.

## Buffer-size sweep

`workers=4`, `hey -z 10s -c 50`:

| buffer | Requests/sec | events ingested (10s) |
|-------:|-------------:|----------------------:|
| 64     | 62,284.99    | 622,300               |
| 1024   | 61,340.47    | 612,848               |
| 8192   | 60,538.90    | 604,939               |

**Buffer size barely moves steady-state throughput** (and larger is marginally
worse). Consistent with the finding above: because workers keep up, the buffers
rarely fill, so their capacity is almost irrelevant to sustained rate. The
buffer's real job is **burst absorption** (smoothing a momentary worker stall,
per DESIGN §2), not raising the steady-state ceiling — and a needlessly large
buffer just costs memory and cache locality. Default `buffer=1024` is a middle
ground: enough to ride out a scheduler hiccup, small enough to stay
cache-friendly and to bound worst-case memory at `workers × buffer × sizeof(Event)`.

## Concurrency sweep

`workers=4 buffer=1024`, `hey -z 10s`, varying client concurrency `-c`:

| concurrency | Requests/sec |
|------------:|-------------:|
| 10          | 54,440.03    |
| 25          | 60,125.00    |
| 50          | 60,241.35    |
| 100         | 62,382.60    |
| 200         | 63,699.39    |

Throughput rises with offered concurrency and flattens around **~63.7k RPS**
near `c=200` — the point where the 4 shared cores are saturated by server +
generator together. At `c=10` there aren't enough in-flight requests to keep the
server busy, so it's client-limited, not server-limited.

## How to reproduce

```bash
# Full sweep (uses hey if installed, else the bundled Go loadgen):
./loadtest/run.sh

# Single headline run by hand:
go build -o /tmp/analytics ./cmd/analytics
/tmp/analytics -addr :8080 -workers 4 -buffer 1024 &
hey -z 15s -c 100 -m POST -H 'Content-Type: application/json' \
  -d '{"event_type":"click","user_id":"u1","timestamp":0,"properties":{"page":"home"}}' \
  http://127.0.0.1:8080/event
curl -s http://127.0.0.1:8080/stats
```

## Interpreting these for the résumé claim

The claim is "50k+ RPS in load testing." The measured, reproducible figure is
**61,389 RPS** sustained (peaks to ~63.7k), with zero errors and a 6.5 ms p99,
on a shared 4-core box. That clears 50k with margin *and* the number is
defensible because the bottleneck analysis above explains exactly what limits it
and why more workers/buffer don't raise it.
