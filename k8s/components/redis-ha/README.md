# Highly available Redis on Kubernetes (Redis Sentinel)

Owner: **Tan Yan Ting** (Kubernetes deployment). Replication logic: **Benny**, see
[`pkg/replication/README.md`](../../../pkg/replication/README.md).

`k8s/base` runs one Redis pod, so Redis is a single point of failure: if it dies,
every match stops committing moves and no standby can take over. This component
replaces it with the group the proposal describes, a Redis primary watched by
Redis Sentinel:

```
        arcade-redis-0            arcade-redis-1            arcade-redis-2
   ┌────────────────────┐   ┌────────────────────┐   ┌────────────────────┐
   │ redis    :6379  P  │──▶│ redis    :6379  R  │   │ redis    :6379  R  │
   │ sentinel :26379    │   │ sentinel :26379    │   │ sentinel :26379    │
   └────────────────────┘   └────────────────────┘   └────────────────────┘
             └────────── replication ───────────────────────▲
   P = primary, R = replica. Sentinels agree (quorum 2 of 3) that the primary is
   down and promote a replica; any pod can hold any role.
```

| Concern | How it is solved |
|---|---|
| Who is primary | Not fixed. The `config` init container (`scripts/init.sh`) asks the Sentinels on the other pods. A brand-new cluster (no Sentinel answers) starts with pod 0 as primary. |
| Stable addresses | Headless Service `arcade-redis-headless`: every pod is `arcade-redis-<i>.arcade-redis-headless`. Replicas and Sentinels announce these names (`replica-announce-ip`, `announce-hostnames`), because pod IPs change when a pod is recreated. |
| A restarted primary must not overwrite newer data | If the Sentinels still list a restarting pod as primary, `init.sh` waits until they have promoted a replica, then starts the pod as a replica. |
| Fencing epoch survives a Redis failover | Gameservers run with `REDIS_MIN_REPLICA_ACKS=1`: every epoch claim and snapshot waits (`WAIT`) until a replica has it, so the promoted replica always has the latest epoch. |
| Isolated old primary | `min-replicas-to-write 1`: a primary cut off from both replicas refuses writes. |
| Gameservers find the primary | `REDIS_SENTINELS` (overrides `REDIS_ADDR`). The go-redis Sentinel client reconnects to the new primary after a failover; a move's commit is retried for up to 3 s, so play pauses instead of failing. |
| Node failure | Soft anti-affinity spreads the three pods over nodes; a PodDisruptionBudget allows only one voluntary disruption at a time. |

Sentinel settings: `down-after-milliseconds 1000`, `failover-timeout 5000`
(set in `statefulset-patch.yaml`). A failover normally completes in about 2 s.

## Requirements

* **Gameserver image built from `feat/redis-ha`** (or `main` after it is merged).
  Older images ignore `REDIS_SENTINELS`.
* `redis:7.4-alpine` (pulled automatically).

## Deploy

```powershell
# 1. Images from the feat/redis-ha code (repo root)
docker build -f deployments/Dockerfile.gameserver -t arcade/gameserver:dev .
docker build -f deployments/Dockerfile.proxy      -t arcade/proxy:dev .

# 2. Start clean. serviceName and podManagementPolicy of a StatefulSet cannot
#    change, so the old single-Redis StatefulSet must go.
kubectl delete namespace arcade --ignore-not-found
kubectl apply -k k8s/overlays/ha

# 3. Wait until all three Redis pods show 2/2 (redis + sentinel)
kubectl -n arcade get pods -l app.kubernetes.io/name=arcade-redis -w
```

The gameservers may restart once or twice while Redis forms: until a replica has
connected, the primary refuses writes (`min-replicas-to-write 1`). That is
expected; they settle within about a minute.

To use HA Redis with the load test, add the component to an overlay:

```yaml
resources: [../load]
components: [../../components/redis-ha]
```

## Check it

```powershell
# Which pod is primary (ask any Sentinel)
kubectl -n arcade exec arcade-redis-1 -c sentinel -- redis-cli -p 26379 SENTINEL get-master-addr-by-name arcade

# Role of each pod
0..2 | % { kubectl -n arcade exec arcade-redis-$_ -c redis -- redis-cli ROLE | Select-Object -First 1 }

# Sentinel sees 2 replicas and 2 other Sentinels
kubectl -n arcade exec arcade-redis-0 -c sentinel -- redis-cli -p 26379 SENTINEL master arcade |
  Select-String -Context 0,1 "num-slaves|num-other-sentinels|quorum"

# Gameservers connected through Sentinel
kubectl -n arcade logs arcade-gs-b-0 | Select-String "sentinel master"   # standby logs its Redis source
```

## Failover test

```powershell
.\test\ha\redis-failover.ps1            # kill the Redis primary once, match-000
.\test\ha\redis-failover.ps1 -Runs 3    # three runs, results saved to a CSV
```

It deletes the Redis primary pod and reports how long Sentinel took to promote a
replica, how long until match-000 committed its next move, whether the epoch
survived, and whether the killed pod came back as a replica. Expected: failover
about 2 s, moves resume within about 3 s, epoch unchanged, old pod a replica.

Manual variants:

| Test | Command | Expected |
|---|---|---|
| Kill one replica | `kubectl -n arcade delete pod <replica> --force --grace-period=0` | No pause: one replica is enough for `WAIT 1`. |
| Kill both replicas | delete both replica pods together | Moves stop with `ErrNotReplicated` until one is back (consistency over availability), then continue. |
| Kill Redis primary and a game primary | delete both at once | Redis fails over, then the game standby promotes at a higher epoch. |

## Troubleshooting

* **Sentinel never fails over, log says `+tilt`:** WSL2's clock jumped, which pauses
  Sentinel for 30 s. Fix until the next WSL restart:
  `wsl -u root -e sh -c "echo hyperv_clocksource_tsc_page > /sys/devices/system/clocksource/clocksource0/current_clocksource"`
* **`field is immutable` on apply:** the single-Redis StatefulSet is still there.
  `kubectl -n arcade delete statefulset arcade-redis`, then apply again.
* **Logs of the startup decision:** `kubectl -n arcade logs arcade-redis-1 -c config`.

## Known limitations

* If **all three** Redis pods restart at the same moment (whole cluster down), no
  Sentinel is left to ask, so pod 0 becomes primary with whatever its disk holds.
  Moves acknowledged only by another pod could then be lost.
* The `arcade-redis` ClusterIP Service now spreads over all three pods. The load
  probe reads Redis `INFO` through it, so with HA its Redis figures describe a
  random pod, not necessarily the primary.
* One primary still handles every write, so this adds availability, not
  throughput. Scaling writes needs sharding (Redis Cluster); see the report.
