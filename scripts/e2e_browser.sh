#!/usr/bin/env bash
set -euo pipefail

BROWSER="${1:-firefox}"

echo "=========================================================="
echo " ARCADE Browser E2E Runner: $BROWSER "
echo "=========================================================="

# 1. Kill stale processes and flush old Redis test keys
pkill -9 -f 'gameserver' || true
pkill -9 -f 'proxy' || true
fuser -k 8080/tcp 9090/tcp 8081/tcp 8082/tcp 2>/dev/null || true
redis-cli del "arcade:match:{room0}:epoch" "arcade:match:{room0}:snapshot" "arcade:match:{room0}:stream" > /dev/null 2>&1 || true
mkdir -p bin/

go build -o bin/gameserver ./cmd/gameserver
go build -o bin/proxy ./cmd/proxy

# 2. Boot Ingress Proxy (:8080 client, :9090 admin)
bin/proxy -listen :8080 -admin 127.0.0.1:9090 -insecure-skip-origin-check &
PROXY_PID=$!
sleep 1

# 3. Boot Primary (arcade-gs-a-0)
ROLE=primary POD_NAME=arcade-gs-a-0 MATCH_ID=room0 REDIS_ADDR=127.0.0.1:6379 \
  PEER_STATEFULSET=arcade-gs-b GOVERNING_SERVICE=arcade-gs \
  HTTP_PORT=8081 HEARTBEAT_PORT=9000 PEER_HEARTBEAT_PORT=9001 PEER_HOST=127.0.0.1 \
  PROXY_ADMIN_URL=http://127.0.0.1:9090 \
  bin/gameserver -match=room0 -http=8081 -seed=42 -redis=127.0.0.1:6379 &
PRIMARY_PID=$!
sleep 1

# 4. Boot Standby (arcade-gs-b-0)
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

# 6. Execute the Playwright test via uv
uv run scripts/browser_e2e.py --browser "$BROWSER"

# 7. Cleanup
kill -9 "$BACKUP_PID" "$PROXY_PID" 2>/dev/null || true
echo "[E2E] Finished successfully!"