#!/usr/bin/env bash
# End-to-end failover test with the real binaries as separate processes.
#
#   scripts/e2e.sh crash      # kill -9 the primary, then announce the takeover
#   scripts/e2e.sh graceful   # primary steps down (close 1012), then announce the takeover
#   scripts/e2e.sh discover   # kill -9 the primary, promote the standby, and send NO notification:
#                             # the proxy finds the new primary by asking the pods (GET /role)
#
# Exit status 0 only if no simulated player was disconnected.
set -euo pipefail

MODE="${1:-crash}"
CLIENTS="${CLIENTS:-20}"
DURATION="${DURATION:-12s}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BIN="$(mktemp -d)"
STATE="$BIN/state.json"
PIDS=()

cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; rm -rf "$BIN"; }
trap cleanup EXIT

echo "building..."
(cd "$ROOT" && go build -o "$BIN/" ./cmd/proxy ./cmd/fakegame ./cmd/wsclient)

if [ "$MODE" = discover ]; then
  "$BIN/fakegame" -name A -listen 127.0.0.1:19001 -state "$STATE" -role primary -epoch 1 2>"$BIN/A.log" & A=$!; PIDS+=("$A")
  "$BIN/fakegame" -name B -listen 127.0.0.1:19002 -state "$STATE" -role backup -epoch 0 2>"$BIN/B.log" & B=$!; PIDS+=("$B")
  # no -default-upstream and no notifications: every route comes from GET /role
  "$BIN/proxy" -listen 127.0.0.1:18080 -admin 127.0.0.1:19090 -admin-token s3cret \
      -discover-pods 127.0.0.1:19001 -discover-pods 127.0.0.1:19002 -discover-interval 200ms \
      -insecure-skip-origin-check 2>"$BIN/proxy.log" & P=$!; PIDS+=("$P")
else
  "$BIN/fakegame" -name A -listen 127.0.0.1:19001 -state "$STATE" 2>"$BIN/A.log" & A=$!; PIDS+=("$A")
  "$BIN/fakegame" -name B -listen 127.0.0.1:19002 -state "$STATE" 2>"$BIN/B.log" & B=$!; PIDS+=("$B")
  "$BIN/proxy" -listen 127.0.0.1:18080 -admin 127.0.0.1:19090 -admin-token s3cret \
      -default-upstream 127.0.0.1:19001 -insecure-skip-origin-check 2>"$BIN/proxy.log" & P=$!; PIDS+=("$P")
fi

for port in 19001 19002 18080 19090; do
  for _ in $(seq 50); do curl -fs "http://127.0.0.1:$port/healthz" >/dev/null 2>&1 && break; sleep 0.1; done
done

announce() { # epoch, addr
  for i in $(seq 0 $((CLIENTS - 1))); do
    curl -fs -X POST "http://127.0.0.1:19090/admin/route" -H 'Authorization: Bearer s3cret' \
      -d "{\"match_id\":\"room$i\",\"epoch\":$1,\"primary_addr\":\"$2\"}" >/dev/null
  done
}

"$BIN/wsclient" -url ws://127.0.0.1:18080/ws -clients "$CLIENTS" -duration "$DURATION" -interval 50ms \
  >"$BIN/client.out" 2>&1 & C=$!

sleep 4
echo "t+4s: failing the primary ($MODE)"
if [ "$MODE" = crash ] || [ "$MODE" = discover ]; then
  kill -9 "$A"
else
  curl -fs -X POST http://127.0.0.1:19001/demote >/dev/null
fi
sleep 0.3
if [ "$MODE" = discover ]; then
  echo "t+4.3s: standby B promotes itself (epoch 2); nobody tells the proxy"
  curl -fs -X POST "http://127.0.0.1:19002/promote?epoch=2" >/dev/null
else
  echo "t+4.3s: control plane announces the takeover: epoch 2 -> B"
  announce 2 127.0.0.1:19002
fi

rc=0; wait "$C" || rc=$?
echo "----- wsclient -----"; cat "$BIN/client.out"
echo "----- proxy stats -----"; curl -s http://127.0.0.1:19090/stats; echo
exit "$rc"
