# Two proxies instead of one (`proxy-ha`)

Owner: **Brian** (ingress proxy). Pattern and style follow
[`../redis-ha`](../redis-ha/README.md).

`k8s/base` runs **one** `arcade-proxy` pod. Every player's WebSocket goes through
it, so if that pod dies every player is disconnected until Kubernetes starts a new
one. This component runs **two**, so a proxy failure only affects the players that
were connected to that one.

```
 players ──▶ Service arcade-proxy (LoadBalancer :8080) ──┬──▶ arcade-proxy-xxxx  ─┐
                                                         └──▶ arcade-proxy-yyyy  ─┼──▶ game pods
                          each new connection goes to one ready proxy             │   (primary of the match)
                                                                                  ┘
```

```
kubectl apply -k k8s/overlays/ha-full     # Redis HA + 2 proxies, on top of the demo
```

| Concern | How it is handled |
|---|---|
| Can any proxy serve any match? | Yes. A proxy keeps no shared state: its route table is rebuilt by asking the game pods (`GET /role`, `-discover`, see `docs/proxy.md`). Two proxies never have to agree with each other. |
| A proxy crashes | The Service stops sending new connections to it (`/healthz` readiness probe) and Kubernetes restarts it. **Players that were on it lose their socket and must reconnect**; `cmd/cli` does this by itself (`cmd/cli/reconnect.go`). The match state is in Redis, and the server sends the current state to a client when it connects. |
| Rolling update | `maxSurge: 1, maxUnavailable: 0`: the new proxy is ready before an old one is stopped. The old one is told to stop (SIGTERM) and closes its clients with `1001 going away`, which `cmd/cli` treats as "reconnect". |
| Node drain / upgrade | `PodDisruptionBudget` (`minAvailable: 1`): at most one proxy is taken away voluntarily at a time. |
| Both proxies on one node | Soft pod anti-affinity prefers different nodes, but still schedules both on a single-node cluster such as Docker Desktop. |

## Known limits (read before claiming more than this)

1. **A proxy failure still drops its players' sockets.** A second proxy cannot take
   over another proxy's open WebSocket. What improves is the blast radius (about half
   the players instead of all) and the recovery (reconnect to the other proxy
   straight away instead of waiting for a restart). Clients must reconnect: `cmd/cli`
   does, the browser/`wsclient` test tools do not.
2. **Takeover notices reach only one proxy.** The failure detector sends
   `POST /admin/route` to `http://arcade-proxy:9090` (the Service), which picks *one*
   proxy. The other proxy learns about the new primary through discovery instead
   (it polls `/role` every second, and immediately when one of its players loses its
   primary), so it can be somewhat slower than the proxy that got the notice.
   Fanning the notice out to every proxy would need a change in
   `pkg/detector/detector.go` (resolve a headless Service and POST to each address);
   not done.
3. **Not yet measured on a cluster.** The manifests render (`kustomize build`) and
   the reconnect logic is tested against the real proxy binary on one machine, but a
   proxy-failure run on Docker Desktop/kind with `wsclient` has not been done. Do not
   quote a failover time for a proxy failure.
4. **The Service in front is the next shared piece.** On a cloud provider a
   LoadBalancer is itself redundant; on Docker Desktop it is one machine.
