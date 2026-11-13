# ARCADE on Kubernetes: fleet layout, role assignment & service discovery

Owner: **Tan Yan Ting** (Phase 1: Kubernetes orchestration & fleet lifecycle)

```
 clients ──WebSocket──▶ arcade-proxy   (Deployment, LoadBalancer :8080)
                             │  finds the primary of match-<i> by pod label or GET /role
                             ▼
               headless Service "arcade-gs"   (clusterIP: None → stable pod DNS names)

   pair 0:  arcade-gs-a-0 (ROLE=primary) ◀── UDP :9000 heartbeat ──▶ arcade-gs-b-0 (ROLE=backup)
   pair 1:  arcade-gs-a-1 (ROLE=primary) ◀────────────────────────▶ arcade-gs-b-1 (ROLE=backup)
   ...      pair i serves match-<i>; scale both StatefulSets to N for N matches
                             │
                             │  snapshot commits, fenced by Lua epoch scripts
                             ▼
               arcade-redis   (StatefulSet, append-only file on a PersistentVolume)
```

## Design

| Concern | How it is solved | Where |
|---|---|---|
| Paired Primary/Standby pods | Two StatefulSets with the same replica count: `arcade-gs-a` (pods start `ROLE=primary`) and `arcade-gs-b` (`ROLE=backup`). Pod `a-i` and pod `b-i` form **pair i** and serve **`match-<i>`** (zero-padded, e.g. `match-007`). | `base/gameserver-a.yaml`, `base/gameserver-b.yaml` |
| Deterministic pod-to-pod UDP | One **headless Service** `arcade-gs` governs both sets, so every pod has a stable DNS name `<pod>.arcade-gs.<ns>.svc.cluster.local`. A pod derives its partner's name from its own ordinal (`a-7` → `b-7`) and sends heartbeats to `:9000/udp` there. `publishNotReadyAddresses` lets partners resolve each other while still starting. | `base/gameserver-service.yaml`, `pkg/fleet/identity.go` |
| Runtime role assignment | `ROLE` is only the *initial* role. At startup a would-be primary asks its partner (`GET /role`); if the partner was promoted while this pod was down, it **starts as backup** instead of taking the match back. Without this check a restarted primary would claim a new fencing epoch and fence the healthy promoted pod, causing a second failover. Redis epoch fencing still guarantees safety if two pods race. | `pkg/fleet/arbitrate.go` |
| Live role visibility | Each pod patches its own labels `arcade.io/role`, `arcade.io/match` and annotation `arcade.io/epoch` through the API server (plain HTTPS, no client-go). RBAC lets it `get`/`patch` pods in its namespace only. | `pkg/fleet/labeler.go`, `base/rbac.yaml` |
| Scaling | `kubectl scale sts arcade-gs-a arcade-gs-b --replicas=N` gives N matches. `podManagementPolicy: Parallel` starts 100 pairs at once instead of one by one. | |
| Fault isolation | Soft pod anti-affinity with `matchLabelKeys: [apps.kubernetes.io/pod-index]`: `a-i` avoids only its own partner `b-i`, so on a multi-node cluster a node failure never takes out both halves of a pair. Needs Kubernetes ≥ 1.31. | `base/gameserver-*.yaml` |
| Small, hardened images | Multi-stage builds that cross-compile natively for each `--platform` and run on `distroless/static:nonroot` (~16 MB). Pods run as UID 65532 with a read-only root filesystem and all capabilities dropped (Pod Security "restricted" for our own pods). | `../deployments/` |
| Health | `startupProbe`/`livenessProbe` → `/healthz`; `readinessProbe` → `/readyz` (engine initialised). | |
| Go in containers | `GOMAXPROCS`/`GOMEMLIMIT` come from the container limits (Go 1.24 does not read cgroup limits itself). | |
| Redis | StatefulSet with append-only persistence on a PersistentVolume: the fencing epoch must survive a Redis restart (see `pkg/replication/README.md`). | `base/redis.yaml` |

Ports: `8081/tcp` (metrics, `/healthz`, `/readyz`, `/role`, `/debug/pprof`), `9000/udp` (heartbeat).

### Layout

```
k8s/
├── base/                      # Redis + 1 pair + proxy (kubectl apply -k k8s/base)
├── overlays/demo/             # 3 pairs, each primary self-plays 1 move/s (presentation)
├── overlays/load/             # self-play at 5 moves/s per match + more Redis headroom (Phase 2)
├── components/network-policy/ # optional least-privilege NetworkPolicies
├── load/loadprobe-job.yaml    # in-cluster measurement Job (used by test/load/run-load.*)
└── kind-cluster.yaml          # 3-node kind cluster, 250 pods/node (50/100-pair tests)
```

## Quick start: Docker Desktop (Windows, PowerShell)

1. In Docker Desktop, go to **Settings → Kubernetes → Enable Kubernetes**. Use the default single-node cluster, then check `kubectl config current-context` shows `docker-desktop`.
2. Build the images from the repo root. Docker Desktop's Kubernetes uses the same image store, so you don't need to push anything:
   ```powershell
   docker build -f deployments/Dockerfile.gameserver -t arcade/gameserver:dev .
   docker build -f deployments/Dockerfile.proxy      -t arcade/proxy:dev .
   docker build -f deployments/Dockerfile.loadprobe  -t arcade/loadprobe:dev .
   ```
3. Deploy (the demo overlay runs 3 pairs that self-play):
   ```powershell
   kubectl apply -k k8s/overlays/demo
   kubectl -n arcade get pods -o wide -L arcade.io/role,arcade.io/match -w
   ```
   You should see `arcade-gs-a-*` labelled `primary` and `arcade-gs-b-*` labelled `backup`, each pair sharing a `match-00x` label.
4. Look around:
   ```powershell
   kubectl -n arcade logs arcade-gs-a-0 | Select-String "FLEET|REPLICATION|LOADGEN"
   kubectl -n arcade port-forward pod/arcade-gs-b-0 8081:8081      # in a second window
   curl.exe http://localhost:8081/role                               # {"role":"backup","match_id":"match-000",...}
   kubectl -n arcade get pod arcade-gs-a-0 -o jsonpath="{.metadata.annotations.arcade\.io/epoch}"
   ```
5. Crash a primary and watch Kubernetes restart it. The restarted pod resumes the match from Redis at a higher epoch:
   ```powershell
   kubectl -n arcade delete pod arcade-gs-a-1 --grace-period=0 --force
   kubectl -n arcade logs arcade-gs-a-1 | Select-String "Resuming|epoch"
   ```
6. Clean up: `kubectl delete namespace arcade`.

> Docker Desktop's single node allows **110 pods**. That's enough for the demo and for 10 pairs, but not for 50 pairs (100 gameserver pods + system pods) or 100 pairs (200 pods). Use kind for those runs.

## Large runs: kind (3 nodes, 250 pods per worker)

```powershell
winget install Kubernetes.kind          # once
kind create cluster --config k8s/kind-cluster.yaml
kind load docker-image arcade/gameserver:dev arcade/proxy:dev arcade/loadprobe:dev --name arcade
kubectl apply -k k8s/overlays/load
.\test\load\run-load.ps1                 # 10, 50, 100 pairs; see test/load/README.md
kind delete cluster --name arcade        # when done
```

Give Docker Desktop at least 4 CPUs and 6 GB of memory (**Settings → Resources**). In our test, 100 pairs used about 3 GB of RSS.

## For teammates: integrating with the fleet

All of this lives in `pkg/fleet`. `cmd/gameserver/main.go` already resolves the identity, runs the startup arbitration and serves `/role`.

**Marcus (heartbeat & promotion):**
```go
identity                       // fleet.Identity, resolved in main.go
identity.HeartbeatListenAddr() // ":9000": net.ListenUDP here
identity.PeerHeartbeatAddr()   // "arcade-gs-b-7.arcade-gs.arcade.svc.cluster.local:9000": net.DialUDP here
identity.Standalone()          // true when running outside Kubernetes (no partner)

// after store.TakeOver(...) succeeds on the standby:
status.Promote(epoch)          // updates GET /role, the arcade.io/role label and arcade_fleet_is_primary
```

**Brian (proxy):** to find the current primary of `match-<i>`, either
- call `GET http://arcade-gs-a-<i>.arcade-gs:8081/role` and the same on `b-<i>`, and pick the one with `"role":"primary"` and the higher `epoch`, or
- list or watch pods with the label selector `arcade.io/match=match-<i>,arcade.io/role=primary` (the `arcade-proxy` ServiceAccount already has `get/list/watch` on pods).

The proxy Deployment already gets `GAMESERVER_SERVICE`, `GAMESERVER_SETS` and `GAMESERVER_HTTP_PORT`. If the game traffic uses a port other than 8081, add it to the gameserver container ports and to `base/gameserver-service.yaml`.

**Sam (chaos & metrics):**
- To crash a primary: `kubectl -n arcade delete pod arcade-gs-a-<i> --grace-period=0 --force`, or `docker kill` its container.
- To simulate a network partition: apply the optional NetworkPolicies, or add a deny policy for one pod. This only works on a CNI that enforces policies.
- Every gameserver pod carries `prometheus.io/scrape|port|path` annotations.

**Benny (replication):** Redis is at `REDIS_ADDR=arcade-redis:6379`, with AOF persistence on a PersistentVolume. If Redis isn't ready yet, a primary keeps retrying for 60 s instead of crash-looping (`cmd/gameserver/fleet.go`).

## Troubleshooting

| Symptom | Fix |
|---|---|
| `ErrImageNeverPull` / `ImagePullBackOff` for `arcade/*:dev` | On Docker Desktop, build the images again. On kind, run `kind load docker-image ...` again. |
| Pods `Pending`: `Too many pods` | You've hit the 110 pods-per-node limit. Use the kind cluster. |
| Gameserver logs `Redis not reachable yet` | Normal for the first few seconds of a fresh deploy. If it persists, check `kubectl -n arcade get pods -l app.kubernetes.io/name=arcade-redis`. |
| Labels never change from the template values | Check that `rbac.yaml` was applied. The pod logs `[FLEET] label patch attempt ... failed` with the reason. |
| `unknown field "matchLabelKeys"` | Your cluster is older than 1.31. Delete the `matchLabelKeys` lines; anti-affinity then just spreads pods. |

## Known limitations

- Redis is a single instance, so it's a single point of failure for replication. Redis Sentinel or Cluster would be the next step; all keys of a match already share one hash slot.
- The base deployment runs one `arcade-proxy` pod, so the proxy is a single point of failure for connections. `kubectl apply -k k8s/overlays/ha-full` runs two (see `components/proxy-ha/README.md` for what that does and does not fix).
- A standby currently idles until the failure detector (`pkg/detector`) lands. Its measured footprint is therefore a lower bound.
- `ROLE` stays in each pod spec as the initial role. After a failover, the live role is the label and `/role`, not the StatefulSet the pod belongs to.
