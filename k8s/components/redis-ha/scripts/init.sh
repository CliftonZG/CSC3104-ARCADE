#!/bin/sh
# Runs as the init container of every arcade-redis pod. It decides whether this
# pod starts as the Redis primary or as a replica, then writes redis.conf and
# sentinel.conf into the shared /conf volume.
#
# Which pod is primary changes after every Sentinel failover, so we cannot
# hard-code "pod 0 is primary". Instead we ask the Sentinels already running on
# the other pods. Only when none answers (a brand-new cluster) is pod 0 the
# primary.
set -eu

MASTER_NAME="${MASTER_NAME:-arcade}"
SERVICE="${HEADLESS_SERVICE:-arcade-redis-headless}"
REPLICAS="${REPLICAS:-3}"
CONF_DIR="${CONF_DIR:-/conf}"
DOWN_AFTER_MS="${DOWN_AFTER_MS:-1000}"
FAILOVER_TIMEOUT_MS="${FAILOVER_TIMEOUT_MS:-5000}"
# Allows the local test to replace ".svc.cluster.local" names; not set in Kubernetes.
DOMAIN="${DOMAIN:-$SERVICE.$POD_NAMESPACE.svc.cluster.local}"

STS="${POD_NAME%-*}"           # arcade-redis-2 -> arcade-redis
SELF="$POD_NAME.$DOMAIN"       # stable DNS name from the headless Service

# Prints the number of replicas the Sentinels know for the primary. A Sentinel
# only learns replicas from a running primary, so 0 means this group has never
# had a working primary with replicas: nothing could be promoted instead.
known_replicas() {
  i=0
  while [ "$i" -lt "$REPLICAS" ]; do
    peer="$STS-$i.$DOMAIN"
    if [ "$peer" != "$SELF" ]; then
      n=$(timeout 2 redis-cli -h "$peer" -p 26379 --raw SENTINEL master "$MASTER_NAME" 2>/dev/null |
            awk 'prev == "num-slaves" { print; exit } { prev = $0 }' || true)
      if [ -n "$n" ]; then
        echo "$n"
        return
      fi
    fi
    i=$((i + 1))
  done
  echo 0
}

# Prints the primary according to the first Sentinel on another pod that answers.
ask_sentinels() {
  i=0
  while [ "$i" -lt "$REPLICAS" ]; do
    peer="$STS-$i.$DOMAIN"
    if [ "$peer" != "$SELF" ]; then
      addr=$(timeout 2 redis-cli -h "$peer" -p 26379 --raw \
               SENTINEL get-master-addr-by-name "$MASTER_NAME" 2>/dev/null | head -n 1 || true)
      if [ -n "$addr" ]; then
        echo "$addr"
        return
      fi
    fi
    i=$((i + 1))
  done
}

master=$(ask_sentinels)

# This pod was the primary and restarted before the Sentinels noticed it was
# gone. Starting as primary now could replace data the replicas already
# acknowledged with this pod's older copy, so wait for the Sentinels to promote
# a replica first. If they never do (no healthy replica), this pod's data is
# the best there is and it starts as primary.
tries=0
if [ "$master" = "$SELF" ] && [ "$(known_replicas)" = "0" ]; then
  echo "init: new cluster, no replicas known yet: no failover to wait for"
  tries=10
fi
while [ "$master" = "$SELF" ] && [ "$tries" -lt 10 ]; do
  echo "init: sentinels still list this pod as primary; waiting for failover"
  sleep 2
  master=$(ask_sentinels)
  tries=$((tries + 1))
done

if [ -n "$master" ]; then
  echo "init: sentinels report primary $master"
else
  master="$STS-0.$DOMAIN"
  echo "init: no sentinel reachable, new cluster: primary is $master"
fi

cat > "$CONF_DIR/redis.conf" <<EOF
port 6379
dir /data
protected-mode no
maxclients 10000
# The fencing epoch must survive a restart (pkg/replication/README.md).
appendonly yes
appendfsync everysec
save ""
# Replicas and Sentinels refer to each other by stable DNS name, not pod IP,
# because pod IPs change when a pod is recreated.
replica-announce-ip $SELF
# A primary that has lost every replica stops accepting writes, so an isolated
# old primary cannot keep diverging after Sentinel has promoted a replica.
min-replicas-to-write 1
min-replicas-max-lag 5
EOF

if [ "$master" = "$SELF" ]; then
  echo "init: $POD_NAME starts as PRIMARY"
else
  echo "replicaof $master 6379" >> "$CONF_DIR/redis.conf"
  echo "init: $POD_NAME starts as REPLICA of $master"
fi

# Sentinel rewrites its own config file, so it must be writable.
cat > "$CONF_DIR/sentinel.conf" <<EOF
port 26379
dir /tmp
# Clients come from other pods, so do not restrict Sentinel to localhost.
protected-mode no
sentinel resolve-hostnames yes
sentinel announce-hostnames yes
sentinel announce-ip $SELF
sentinel monitor $MASTER_NAME $master 6379 2
sentinel down-after-milliseconds $MASTER_NAME $DOWN_AFTER_MS
sentinel failover-timeout $MASTER_NAME $FAILOVER_TIMEOUT_MS
sentinel parallel-syncs $MASTER_NAME 1
EOF

chmod 0666 "$CONF_DIR/redis.conf" "$CONF_DIR/sentinel.conf"
