# Phase 2: horizontal scalability & cluster resource profiling

Owner: **Tan Yan Ting**

**Question:** how do CPU, memory and move latency behave as the fleet grows from 10 to 50 to 100 concurrent Uno matches, each served by a Primary/Standby pod pair?

## Method

| Element | Choice | Why |
|---|---|---|
| Workload | Every primary self-plays one legal move every **200 ms** (5 moves/s per match, `k8s/overlays/load`). The bot is `pkg/loadgen`. "Endless" policy: a player never plays their last two cards, so matches never finish and the load stays steady over the window. | Each move goes through `Game.ApplyAction`, so all move hooks run: the Lua-fenced Redis commit, the metrics hook and move logging. The measured cost is the real per-move cost of a primary. It doesn't depend on the proxy, which isn't built yet. |
| Fleet sizes | 10, 50 and 100 pairs (20 / 100 / 200 gameserver pods) | Required by the proposal |
| Measurement | The `cmd/loadprobe` Job runs inside the cluster and scrapes every pod's `/metrics` twice, `-window` apart (default 60 s, after a 30 s warm-up). It reaches each pod by its stable headless-Service DNS name, so it needs no kubectl, port-forward or metrics-server. | Measures each process individually. Pairs are defined by the fleet layout (`a-i` + `b-i`), and roles come from `arcade_fleet_is_primary`. |
| CPU | Δ`process_cpu_seconds_total` / Δt per pod, summed per pair | Kernel-accounted CPU time for the process |
| Memory | `process_resident_memory_bytes` (RSS) and `go_memstats_heap_inuse_bytes` | RSS is what the node pays for; heap shows how much of it is Go data |
| Latency | Quantiles of Δ`arcade_loadgen_apply_latency_seconds` (whole move incl. hooks) and Δ`uno_replication_snapshot_sync_duration_ms` (Redis commit only), merged over all pods | Shows whether the shared Redis becomes a bottleneck as N grows |
| Shared Redis | `INFO` sampled at both ends: CPU, memory, ops/s, clients, keys | Redis is the only component every pair shares |
| pprof | After each step, a heap profile and a 15 s CPU profile of `arcade-gs-a-0` (primary) and `arcade-gs-b-0` (standby) via `/debug/pprof` | Shows where the per-pair cost goes |

## Running it

Prerequisites: images built and the cluster up (see `k8s/README.md`). For 50 and 100 pairs use the kind cluster, because a single-node cluster caps out at 110 pods. For charts, install Python with `pip install matplotlib`.

```powershell
# Windows / PowerShell
.\test\load\run-load.ps1                                     # 10, 50, 100 pairs, 60 s windows
.\test\load\run-load.ps1 -Pairs 10 -Window 30s -Warmup 15     # quick smoke test
.\test\load\run-load.ps1 -Pairs 10,50,100 -MoveInterval 20ms  # stress: 50 moves/s per match
```
```bash
# Linux / macOS / Git Bash
test/load/run-load.sh -p "10 50 100"
```

Each run writes to `test/load/results/<timestamp>/` (git-ignored):

| File | Content |
|---|---|
| `pairs-<N>.json` | The probe report: per-pod and per-pair results, distributions, latency quantiles, Redis stats |
| `pairs-<N>.log` | The probe's human-readable table |
| `pairs-<N>-pods.txt`, `pairs-<N>-kubectl-top.txt` | Pod placement and roles; `kubectl top` as a cross-check if metrics-server is installed |
| `pairs-<N>-arcade-gs-{a,b}-0-{cpu,heap}.pb.gz` | pprof profiles: `go tool pprof -http=:0 <file>` |
| `summary.csv`, `summary.md` | One row/column per fleet size, ready for the report |
| `fig1_per_pair_footprint.png` | CPU and RSS per pair, split into primary and standby |
| `fig2_fleet_scaling.png` | Total CPU and memory against an ideal linear-scaling line |
| `fig3_latency_and_redis.png` | Move latency p50/p99 and shared Redis CPU |

To re-render charts later: `python test/load/plot_results.py test/load/results/<run>`.

## Validation run (sandbox, *not* the final numbers)

The whole pipeline was validated end-to-end on a deliberately small machine: k3s v1.32 on one node with **2 vCPU and 8 GB RAM**, running the same manifests and code. Use these numbers only as a sanity check. Re-run on the team's cluster for the report.

Settings: `-MoveInterval 100ms` (10 moves/s per match, twice the default rate), 30 s windows and 20–30 s warm-up.

| Metric | 10 pairs | 50 pairs | 100 pairs |
|---|---:|---:|---:|
| Pairs measured | 10 | 50 | 100 |
| CPU / pair (mcores, mean) | 5.36 | 4.74 | 4.28 |
| CPU / pair (mcores, p95) | 5.99 | 5.57 | 4.88 |
| RSS / pair (MiB, mean) | 28.65 | 28.80 | 29.52 |
| RSS / pair (MiB, p95) | 34.11 | 29.61 | 29.94 |
| Primary CPU (mcores) | 5.12 | 4.48 | 3.98 |
| Standby CPU (mcores) | 0.23 | 0.26 | 0.29 |
| Primary RSS (MiB) | 16.39 | 16.84 | 17.10 |
| Standby RSS (MiB) | 12.27 | 11.96 | 12.41 |
| Fleet CPU (mcores) | 53.56 | 236.99 | 427.51 |
| Fleet RSS (MiB) | 286.50 | 1440.25 | 2951.46 |
| Moves/s (fleet) | 100.04 | 498.40 | 996.31 |
| Move latency p50 (ms) | 0.42 | 0.52 | 1.05 |
| Move latency p95 (ms) | 0.98 | 2.85 | 26.00 |
| Move latency p99 (ms) | 3.15 | 18.02 | 84.24 |
| Redis commit p50 (ms) | 0.33 | 0.41 | 0.85 |
| Redis commit p99 (ms) | 2.96 | 14.65 | 72.73 |
| Redis CPU (mcores) | 21.30 | 83.98 | 144.90 |
| Redis memory (MiB) | 40.13 | 53.25 | 105.43 |
| Redis ops/s | 600.18 | 2997.88 | 5976.33 |

`kubectl top` (metrics-server) reported about 4–5 mcores and 6–7 MiB per primary pod at 100 pairs. CPU agrees with the probe. Memory is lower because `kubectl top` shows the cgroup *working set*, which excludes the binary's shared, file-backed pages that RSS counts. State which one you quote in the report: RSS is the conservative figure.

Observations that should hold on any cluster (check them against your own run):

1. **Memory per pair is flat** at about 29 MiB RSS (primary about 17 MiB, standby about 12 MiB), so fleet memory grows linearly with the number of matches. Most of it is the Go runtime plus Prometheus and HTTP baseline: the live heap of a match is under 2 MiB, and an Uno state snapshot is about 190 B (binary) or a few KB (JSON).
2. **CPU per pair is tiny and roughly constant** (about 4–5 mcores at 10 moves/s). The CPU profile shows most of a primary's time goes to the synchronous `EVALSHA` Redis commit, JSON snapshot encoding and logging, not the game rules.
3. **The standby is almost free** (under 1 mcore) while it only waits. Once Marcus's detector adds UDP heartbeats and snapshot tailing, re-run to measure the real hot-standby cost.
4. **The shared Redis grows linearly** (about 6 Redis commands per move, because the Lua commit's inner calls count too, and about 1.5–2 mcores per match at 10 moves/s). At 100 pairs (about 1,000 moves/s and 6,000 commands/s) it uses about 0.15 core, far from saturated. Its memory is dominated by the capped snapshot history stream (`MAXLEN ~256` per match).
5. **Tail latency is the first thing to degrade.** p50 stays around 1 ms, but p99 rises from 3 ms (10 pairs) to 84 ms (100 pairs) once the node's 2 vCPUs are oversubscribed (load average above 10, with 200 pods, Redis and the control plane on one node). That reflects node CPU contention, not the per-pair cost, so pod CPU *requests* and node sizing matter more than per-pod limits.
