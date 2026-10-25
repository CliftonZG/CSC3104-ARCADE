#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo " ARCADE Phase 2: Chaos & Downtime Metrics Evaluation     "
echo " Owners: Sam (Metrics/Chaos) & Marcus (Detector)         "
echo "=========================================================="

# 1. Clean previous state
pkill -9 -f 'gameserver' || true
pkill -9 -f 'proxy' || true
fuser -k 8080/tcp 9090/tcp 8081/tcp 8082/tcp 2>/dev/null || true
redis-cli del "arcade:match:{room0}:epoch" "arcade:match:{room0}:snapshot" "arcade:match:{room0}:stream" > /dev/null 2>&1 || true
mkdir -p bin/

go build -o bin/gameserver ./cmd/gameserver
go build -o bin/proxy ./cmd/proxy
go build -o bin/chaos ./cmd/chaos
go build -o bin/wsclient ./cmd/wsclient

# 2. Boot Ingress Proxy
bin/proxy -listen :8080 -admin 127.0.0.1:9090 -insecure-skip-origin-check &
PROXY_PID=$!
sleep 1

# 3. Boot Primary Gameserver (:8081)
ROLE=primary POD_NAME=arcade-gs-a-0 MATCH_ID=room0 REDIS_ADDR=127.0.0.1:6379 \
  PEER_STATEFULSET=arcade-gs-b GOVERNING_SERVICE=arcade-gs \
  HTTP_PORT=8081 HEARTBEAT_PORT=9000 PEER_HEARTBEAT_PORT=9001 PEER_HOST=127.0.0.1 \
  PROXY_ADMIN_URL=http://127.0.0.1:9090 \
  bin/gameserver -match=room0 -http=8081 -seed=42 -redis=127.0.0.1:6379 &
PRIMARY_PID=$!
sleep 1

# 4. Boot Standby Gameserver (:8082)
ROLE=backup POD_NAME=arcade-gs-b-0 MATCH_ID=room0 REDIS_ADDR=127.0.0.1:6379 \
  PEER_STATEFULSET=arcade-gs-a GOVERNING_SERVICE=arcade-gs \
  HTTP_PORT=8082 HEARTBEAT_PORT=9001 PEER_HEARTBEAT_PORT=9000 PEER_HOST=127.0.0.1 \
  PROXY_ADMIN_URL=http://127.0.0.1:9090 \
  bin/gameserver -match=room0 -http=8082 -seed=42 -redis=127.0.0.1:6379 &
BACKUP_PID=$!
sleep 2

# 5. Set initial route on proxy
curl -s -X POST http://127.0.0.1:9090/admin/route \
  -H "Content-Type: application/json" \
  -d '{"match_id":"room0","epoch":1,"primary_addr":"127.0.0.1:8081"}' > /dev/null

echo "[EVAL] System online. Launching 10 simulated clients..."
bin/wsclient -url ws://127.0.0.1:8080/ws -clients 10 -duration 10s -interval 40ms -match-prefix room -players-per-match 10 &
CLIENT_PID=$!

sleep 3

echo "[EVAL] Executing automated fault injection via cmd/chaos on PID $PRIMARY_PID..."
bin/chaos -target "$PRIMARY_PID" -mode sigkill

# Wait for client continuity test to finish
wait $CLIENT_PID

echo ""
echo "=========================================================="
echo " PROMETHEUS TELEMETRY REPORT (from Promoted Standby :8082)"
echo "=========================================================="
METRICS_DATA=$(curl -s http://127.0.0.1:8082/metrics)

echo "--- Failover Downtime Histogram (uno_cluster_failover_downtime_ms) ---"
echo "$METRICS_DATA" | grep -E '^uno_cluster_failover_downtime_ms' || echo "No downtime samples found"

echo ""
echo "--- Replicated Snapshot Sync Duration (uno_replication_snapshot_sync_duration_ms) ---"
echo "$METRICS_DATA" | grep -E '^uno_replication_snapshot_sync_duration_ms_count' || true

echo ""
echo "--- Role Transitions on Standby (arcade_fleet_role_transitions_total) ---"
echo "$METRICS_DATA" | grep -E '^arcade_fleet_role_transitions_total' || true

echo ""
echo "--- Live Primary Status (arcade_fleet_is_primary) ---"
echo "$METRICS_DATA" | grep -E '^arcade_fleet_is_primary' || true

# Cleanup
kill -9 "$BACKUP_PID" "$PROXY_PID" 2>/dev/null || true
echo ""
echo "[EVAL] Chaos & Metrics Evaluation Complete!"