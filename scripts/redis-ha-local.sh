#!/usr/bin/env bash
# Local highly available Redis for pkg/replication: 1 primary, 2 replicas and
# 3 Sentinels (master name "arcade") on 127.0.0.1. Works in WSL or Linux.
#
#   scripts/redis-ha-local.sh up       start (primary :6390, replicas :6391 :6392, sentinels :26390-26392)
#   scripts/redis-ha-local.sh status   show who is primary and the replication state
#   scripts/redis-ha-local.sh kill     crash the current primary (Sentinel promotes a replica in ~1-2s)
#   scripts/redis-ha-local.sh down     stop everything and delete the data
#
# Point the gameserver at the Sentinels instead of a single Redis:
#   -redis-sentinels 127.0.0.1:26390,127.0.0.1:26391,127.0.0.1:26392 -redis-acks 1
set -euo pipefail

DIR="${REDIS_HA_DIR:-/tmp/arcade-redis-ha}"
DATA_PORTS=(6390 6391 6392)
SENTINEL_PORTS=(26390 26391 26392)

primary() {
  redis-cli -p "${SENTINEL_PORTS[0]}" SENTINEL get-master-addr-by-name arcade 2>/dev/null | paste -sd: -
}

up() {
  mkdir -p "$DIR"
  for p in "${DATA_PORTS[@]}"; do
    mkdir -p "$DIR/$p"
    args=(--port "$p" --dir "$DIR/$p" --daemonize yes --logfile "$DIR/$p/redis.log"
          --appendonly yes --appendfsync everysec --save ""
          # A primary cut off from every replica stops accepting writes, so an
          # isolated old primary cannot keep diverging after a failover.
          --min-replicas-to-write 1 --min-replicas-max-lag 5)
    [ "$p" != "${DATA_PORTS[0]}" ] && args+=(--replicaof 127.0.0.1 "${DATA_PORTS[0]}")
    redis-server "${args[@]}"
  done
  for p in "${SENTINEL_PORTS[@]}"; do
    cat > "$DIR/sentinel-$p.conf" <<EOF
port $p
daemonize yes
logfile $DIR/sentinel-$p.log
dir $DIR
sentinel monitor arcade 127.0.0.1 ${DATA_PORTS[0]} 2
sentinel down-after-milliseconds arcade 1000
sentinel failover-timeout arcade 5000
sentinel parallel-syncs arcade 1
EOF
    redis-server "$DIR/sentinel-$p.conf" --sentinel
  done
  sleep 2
  status
}

status() {
  echo "primary (per Sentinel): $(primary)"
  for p in "${DATA_PORTS[@]}"; do
    role=$(redis-cli -p "$p" ROLE 2>/dev/null | head -1 || true)
    echo "  :$p ${role:-down}"
  done
}

kill_primary() {
  addr=$(primary)
  echo "crashing primary $addr (kill -9)"
  pkill -9 -f "redis-server .*:${addr##*:}( |$)" || true
}

down() {
  for p in "${SENTINEL_PORTS[@]}" "${DATA_PORTS[@]}"; do
    redis-cli -p "$p" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
  done
  rm -rf "$DIR"
  echo "stopped"
}

case "${1:-}" in
  up) up ;;
  status) status ;;
  kill) kill_primary ;;
  down) down ;;
  *) sed -n '2,12p' "$0"; exit 1 ;;
esac
