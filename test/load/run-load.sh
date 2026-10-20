#!/usr/bin/env bash
# Phase 2 load test: horizontal scalability & per-pair resource profiling.
# Owner: Tan Yan Ting.  Windows users: see run-load.ps1 (same steps).
#
# For each fleet size N (default 10, 50, 100 pairs) it:
#   1. scales arcade-gs-a and arcade-gs-b to N replicas (N Primary/Standby pairs)
#   2. waits for every pod to be Ready, then a warm-up period
#   3. runs the in-cluster load probe Job (cmd/loadprobe) for the window
#   4. saves the probe report (JSON + table), `kubectl top` (if metrics-server
#      is installed) and pprof heap/CPU profiles of pair 0 (primary + standby)
# and finally renders charts with plot_results.py.
#
# Usage: test/load/run-load.sh [-p "10 50 100"] [-w 60s] [-u 30] [-i 200ms] [-o DIR]
#   -i  time between self-play moves per match (default 200ms = 5 moves/s)
#                              [-k "kubectl"] [--skip-apply] [--no-pprof]
set -euo pipefail

PAIRS="10 50 100"; WINDOW=60s; WARMUP=30; INTERVAL=200ms; OUT=""; KUBECTL="kubectl"
NS=arcade; APPLY=1; PPROF=1
HERE="$(cd "$(dirname "$0")" && pwd)"; ROOT="$(cd "$HERE/../.." && pwd)"

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p) PAIRS="$2"; shift 2;;
    -w) WINDOW="$2"; shift 2;;
    -u) WARMUP="$2"; shift 2;;
    -i) INTERVAL="$2"; shift 2;;
    -o) OUT="$2"; shift 2;;
    -k) KUBECTL="$2"; shift 2;;
    --skip-apply) APPLY=0; shift;;
    --no-pprof) PPROF=0; shift;;
    -h|--help) sed -n '2,16p' "$0"; exit 0;;
    *) echo "unknown argument $1" >&2; exit 2;;
  esac
done
OUT="${OUT:-$HERE/results/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT"
k() { $KUBECTL -n "$NS" "$@"; }
log() { printf '\n[%s] %s\n' "$(date +%H:%M:%S)" "$*"; }

log "Results -> $OUT"
{
  echo "date: $(date -Iseconds)"; echo "pairs: $PAIRS window: $WINDOW warmup: ${WARMUP}s interval: $INTERVAL"
  $KUBECTL version 2>/dev/null || true
  $KUBECTL get nodes -o wide
  $KUBECTL describe nodes | grep -A6 -E '^Capacity:|^Allocatable:' || true
} > "$OUT/environment.txt" 2>&1

if [[ $APPLY == 1 ]]; then
  log "Applying k8s/overlays/load"
  $KUBECTL apply -k "$ROOT/k8s/overlays/load"
fi
# Always set the move rate explicitly: an env var added by `kubectl set env`
# survives later `kubectl apply`s, so relying on the overlay's value is unsafe.
log "Setting AUTOPLAY_INTERVAL=$INTERVAL (one self-play move per match per interval)"
k set env statefulset/arcade-gs-a statefulset/arcade-gs-b AUTOPLAY_INTERVAL="$INTERVAL"
k rollout status statefulset/arcade-redis --timeout=5m

for N in $PAIRS; do
  log "=== $N pairs ($((2 * N)) gameserver pods) ==="
  k scale statefulset/arcade-gs-a statefulset/arcade-gs-b --replicas="$N"
  k rollout status statefulset/arcade-gs-a --timeout=15m
  k rollout status statefulset/arcade-gs-b --timeout=15m
  # Poll readyReplicas instead of `kubectl wait pod -l ...`: when scaling down,
  # that would also wait on pods that are being deleted and hang.
  deadline=$((SECONDS + 900))
  until [[ "$(k get sts arcade-gs-a -o jsonpath='{.status.readyReplicas}')" == "$N" && \
           "$(k get sts arcade-gs-b -o jsonpath='{.status.readyReplicas}')" == "$N" ]]; do
    (( SECONDS < deadline )) || { echo "pods not Ready after 15 minutes" >&2; exit 1; }
    sleep 2
  done
  log "All pods Ready; warming up ${WARMUP}s"
  sleep "$WARMUP"

  k get pods -o wide -L arcade.io/role,arcade.io/match > "$OUT/pairs-$N-pods.txt"
  k top pods --no-headers > "$OUT/pairs-$N-kubectl-top.txt" 2>&1 || echo "(kubectl top unavailable: install metrics-server)" > "$OUT/pairs-$N-kubectl-top.txt"

  JOB="arcade-loadprobe-$N"
  k delete job "$JOB" --ignore-not-found >/dev/null
  sed -e "s/__PAIRS__/$N/g" -e "s/__WINDOW__/$WINDOW/g" "$ROOT/k8s/load/loadprobe-job.yaml" | $KUBECTL apply -f - >/dev/null
  log "Load probe running ($WINDOW window)"
  k wait --for=condition=complete "job/$JOB" --timeout=20m >/dev/null || { k logs "job/$JOB" || true; echo "probe failed" >&2; exit 1; }
  k logs "job/$JOB" > "$OUT/pairs-$N.log"
  sed -n '/=== LOADPROBE JSON BEGIN ===/,/=== LOADPROBE JSON END ===/p' "$OUT/pairs-$N.log" | sed '1d;$d' > "$OUT/pairs-$N.json"
  sed -n '/ARCADE load probe/,/=== LOADPROBE JSON BEGIN ===/p' "$OUT/pairs-$N.log" | sed '$d'

  if [[ $PPROF == 1 ]]; then
    for pod in arcade-gs-a-0 arcade-gs-b-0; do
      k port-forward "pod/$pod" 18081:8081 >/dev/null 2>&1 &
      PF=$!
      for _ in $(seq 1 50); do curl -fs -o /dev/null http://127.0.0.1:18081/healthz && break; sleep 0.2; done
      curl -fs -o "$OUT/pairs-$N-$pod-heap.pb.gz" "http://127.0.0.1:18081/debug/pprof/heap" || echo "heap profile failed for $pod" >&2
      curl -fs -o "$OUT/pairs-$N-$pod-cpu.pb.gz" "http://127.0.0.1:18081/debug/pprof/profile?seconds=15" || echo "cpu profile failed for $pod" >&2
      curl -fs -o "$OUT/pairs-$N-$pod-role.json" "http://127.0.0.1:18081/role" || true
      kill "$PF" 2>/dev/null || true; wait "$PF" 2>/dev/null || true
    done
    log "pprof profiles saved (go tool pprof -top $OUT/pairs-$N-arcade-gs-a-0-cpu.pb.gz)"
  fi
done

if command -v python3 >/dev/null 2>&1 || command -v python >/dev/null 2>&1; then
  log "Rendering charts"
  "$(command -v python3 || command -v python)" "$HERE/plot_results.py" "$OUT" || echo "plotting failed (pip install matplotlib)" >&2
fi
log "Done. Results in $OUT"
