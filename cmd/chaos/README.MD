# Chaos testing

`cmd/chaos` injects failures into an ARCADE gameserver pair and measures whether
the standby takes over. Kubernetes modes use the current `kubectl` context and
target the primary pod of the selected pair.

## Prerequisites

- A running ARCADE Kubernetes deployment with Redis and at least one gameserver
  pair. For a local Docker Desktop cluster, follow the setup in
  [`k8s/README.md`](../../k8s/README.md).
- `kubectl` configured for that cluster, with permission to read/watch pods,
  delete pods, and create ephemeral debug containers. Network fault modes also
  need the debug container's `NET_ADMIN` permissions and an image containing
  `iptables` and `tc` (the default is `nicolaka/netshoot`).
- A healthy pair with one primary and one backup. Pair `0` is
  `arcade-gs-a-0` + `arcade-gs-b-0`; use `-namespace` if the deployment is not
  in the default `arcade` namespace.

Check the active context and pair before running:

```powershell
kubectl config current-context
kubectl -n arcade get pods -l app.kubernetes.io/name=arcade-gameserver `
  -L apps.kubernetes.io/pod-index,arcade.io/role
```

## Run a test

From the repository root, run a single force-delete test:

```powershell
go run ./cmd/chaos -mode k8s-crash -pair 0
```

Try the dual-validation cases, which exercise heartbeat and Redis connectivity:

```powershell
# Cut heartbeats only. Redis snapshots are still arriving, so takeover is suppressed.
go run ./cmd/chaos -mode partition -pair 0 -block udp -duration 10s

# Cut both heartbeats and Redis. The standby should take over.
go run ./cmd/chaos -mode partition -pair 0 -block udp,redis -duration 10s
```

Other network-fault examples:

```powershell
go run ./cmd/chaos -mode loss -pair 0 -percent 10 -duration 10s
go run ./cmd/chaos -mode delay -pair 0 -delay 50ms -jitter 20ms -duration 10s
```

For repeated, clean trials, `-reset` restarts both pods before each run and
`-warmup` allows self-play to resume before the fault is injected:

```powershell
go run ./cmd/chaos -mode partition -pair 0 -block udp,redis `
  -duration 10s -runs 5 -reset -warmup 30s
```

The network-fault debug container removes its rules when its timed script exits.
If the CLI is interrupted, the script still has a cleanup trap; if a fault
persists, inspect the debug container and restart the affected pod.

## Results and options

The command prints one CSV row per run:

```text
run,mode,victim,promoted,recovery_ms,pair_after
```

`recovery_ms` is elapsed time between fault activation and observing the other
pod become primary. A promotion observed before fault activation is reported as
`invalid` and excluded from the recovery summary; this can happen if a promotion
was already underway before the injection. `pair_after` reports whether the
pair healed to one primary and one backup, split-brain, or another state.

Useful flags (all durations accept Go duration syntax such as `10s` or `500ms`):

| Flag | Default | Purpose |
|---|---:|---|
| `-mode` | `sigkill` | `k8s-crash`, `partition`, `loss`, or `delay` for Kubernetes; `sigkill` is local only |
| `-namespace` | `arcade` | Kubernetes namespace |
| `-pair` | `0` | Pair index (`arcade-gs-a-i` / `arcade-gs-b-i`) |
| `-runs` | `1` | Number of injections |
| `-settle` | `3s` | Wait after confirming the pair is healthy before injecting |
| `-window` | `30s` | Wait for takeover in `k8s-crash` mode |
| `-duration` | `10s` | Length of a network fault |
| `-block` | `udp,redis` | Partition targets: `udp`, `redis`, or both |
| `-percent` | `10` | Packet loss percentage |
| `-delay`, `-jitter` | `50ms`, `20ms` | Added network delay and jitter |
| `-dev` | `eth0` | Network interface inside the pod for `tc` |
| `-image` | `nicolaka/netshoot` | Ephemeral debug-container image |
| `-reset` | `false` | Restart both pair pods before each run |
| `-warmup` | `30s` | Self-play warmup after `-reset` |
| `-heal-wait` | `45s` | Wait for the pair to heal before recording its final state |

`-mode sigkill -target <pid>` kills a process on the machine running the
command; it is not a Kubernetes test. Avoid broad process-name targets.