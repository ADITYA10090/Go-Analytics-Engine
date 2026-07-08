#!/usr/bin/env bash
# run.sh — sweep worker count and channel buffer size, recording POST /event
# throughput at each setting. Uses `hey` if available, otherwise falls back to
# the bundled Go loadgen so the sweep is reproducible on any machine.
#
# Usage:
#   ./loadtest/run.sh                 # full sweep
#   DURATION=15s CONC=100 ./loadtest/run.sh
#
# Nothing here fabricates numbers — it prints whatever the machine produces.
set -euo pipefail

cd "$(dirname "$0")/.."

ADDR="${ADDR:-127.0.0.1:8081}"
URL="http://${ADDR}/event"
DURATION="${DURATION:-10s}"
CONC="${CONC:-50}"
BODY='{"event_type":"click","user_id":"u1","timestamp":0,"properties":{"page":"home"}}'

WORKERS_SWEEP="${WORKERS_SWEEP:-1 2 4 8 16}"
BUFFER_SWEEP="${BUFFER_SWEEP:-64 1024 8192}"

echo "building binaries..."
go build -o /tmp/analytics ./cmd/analytics
go build -o /tmp/loadgen ./loadtest/loadgen

have_hey() { command -v hey >/dev/null 2>&1; }

run_one() {
  local workers="$1" buffer="$2"
  /tmp/analytics -addr "${ADDR}" -workers "$workers" -buffer "$buffer" >/tmp/analytics.log 2>&1 &
  local pid=$!
  # wait for readiness
  for _ in $(seq 1 50); do
    if curl -fs "http://${ADDR}/healthz" >/dev/null 2>&1; then break; fi
    sleep 0.1
  done

  echo "----- workers=${workers} buffer=${buffer} -----"
  if have_hey; then
    hey -z "$DURATION" -c "$CONC" -m POST \
      -H "Content-Type: application/json" -d "$BODY" "$URL" \
      | grep -E "Requests/sec|Total:|Status code" || true
  else
    /tmp/loadgen -url "$URL" -c "$CONC" -d "$DURATION" -body "$BODY"
  fi

  # show what the server aggregated
  curl -fs "http://${ADDR}/stats" | head -c 400; echo
  kill "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
}

echo "== sweeping workers (buffer=1024) =="
for w in $WORKERS_SWEEP; do run_one "$w" 1024; done

echo "== sweeping buffer (workers=4) =="
for b in $BUFFER_SWEEP; do run_one 4 "$b"; done

echo "done."
