# ARCADE ingress proxy

`cmd/proxy` is the **data-plane ingress**. It keeps every player's WebSocket open and forwards it to
whichever game pod is currently the **primary** for the player's match. When the control plane announces
a new primary, the proxy moves the player over **without closing the player's socket**.

It never decides who the primary is. It only follows route updates, and only to a **higher epoch**.

```
browser ══ ws ══> proxy ══ ws ══> game pod (primary, epoch 1)      ← replaced
                    ▲   └ ─ ws ─ > game pod (new primary, epoch 2) ← takes over, browser never notices
                    │
   control plane ── POST /admin/route {match_id, epoch, primary_addr}
```

## Layout

| Path | What |
|---|---|
| `cmd/proxy` | the ingress binary |
| `pkg/proxy` | route table, sessions, retargeting, fence, admin API |
| `cmd/fakegame` | stand-in game pod (a shared counter) for demos and tests |
| `cmd/wsclient` | N simulated players; the Phase 2 "client connection continuity" test |
| `scripts/e2e.sh` | real processes, `kill -9` the primary (`crash`), step it down (`graceful`), or promote the standby with no notice at all (`discover`) |
| `scripts/browser_e2e.py` | real Chromium `WebSocket` through a `kill -9` failover |

Requires Go 1.24+ (the repo's `go.mod`) and one new dependency, [`github.com/coder/websocket`](https://github.com/coder/websocket) v1.8.15:
`go get github.com/coder/websocket@v1.8.15`.

## Try it (60 seconds)

```bash
go build -o bin/ ./cmd/...
bin/fakegame -name A -listen 127.0.0.1:9001 -state /tmp/arcade-state.json &
bin/fakegame -name B -listen 127.0.0.1:9002 -state /tmp/arcade-state.json &
bin/proxy -listen :8080 -admin 127.0.0.1:9090 -default-upstream 127.0.0.1:9001 -insecure-skip-origin-check &

bin/wsclient -url ws://127.0.0.1:8080/ws -clients 20 -duration 20s &     # players, via the proxy

# while it runs: kill the primary, then announce the takeover
pkill -9 -f 'fakegame -name A'
for i in $(seq 0 19); do
  curl -s -X POST localhost:9090/admin/route -d "{\"match_id\":\"room$i\",\"epoch\":2,\"primary_addr\":\"127.0.0.1:9002\"}"
done
```

Or just `scripts/e2e.sh crash` / `graceful` / `discover`.

## Interfaces (the parts other people code against)

### Client → proxy
`ws://<proxy>/ws?match=<match_id>&<anything else>`. `match_id` is 1–64 chars of `[A-Za-z0-9_-]`.
The whole query string is forwarded to the game pod unchanged (put `player=` etc. there).
Browsers must come from an allowed origin (`-origin game.example.com`).

### Control plane → proxy (takeover notification) — **proposed, to be agreed with the leader-election owner**
`POST /admin/route` (internal listener, optional `Authorization: Bearer $ARCADE_ADMIN_TOKEN`):

```json
{"match_id": "room7", "epoch": 2, "primary_addr": "game-1.game.svc:8081"}
```

| Result | Meaning |
|---|---|
| `applied` | newer epoch: clients of that match move to the new primary |
| `stale` | `epoch` lower than the current one; ignored (late/reordered message) |
| `duplicate` | same epoch, same address; no-op (safe to retry) |
| `conflict` (HTTP 409) | same epoch, **different** address: two primaries for one epoch, a control-plane bug |

Rules the sender must follow: epochs only ever increase per match; **publish only after the old primary
is fenced** (e.g. after the Redis compare-and-set that wins the epoch succeeded).
`GET /admin/routes` shows the table, `GET /stats` the counters, `GET /healthz` is for probes.

If you would rather use Redis pub/sub than HTTP, call `proxy.ApplyUpdate(proxy.Update{...})` from a
subscriber. **Untested sketch** (go-redis could not be fetched in the sandbox where this was written):

```go
sub := rdb.Subscribe(ctx, "arcade:primary-changes")
for msg := range sub.Channel() {
    var u proxy.Update
    if json.Unmarshal([]byte(msg.Payload), &u) == nil {
        p.ApplyUpdate(u) // stale/duplicate updates are ignored, so redelivery is harmless
    }
}
// Pub/sub is fire-and-forget: after every (re)subscribe, also read the durable
// key match:{id}:primary for each live match, or a missed message is never recovered.
```

### Discovery: the proxy asks the pods (`-discover`)

Takeover notifications can be lost, late, or never sent (a pod that becomes primary while starting up
sends none). With `-discover` the proxy also finds each match's primary itself:

* For every match that has a connected client it calls `GET /role` on the pair's pods every
  `-discover-interval` (default 1s), and immediately when a client has no route or its primary just failed.
* Pod `<set>-<n>` serves `match-<n>`; the address is `<set>-<n>.<service>.<ns>.svc.cluster.local:<port>`,
  the same string the failure detector announces, so a notification and a poll for the same epoch agree.
* A pod counts only if it reports `"role":"primary"` and `"ready":true`. Among those the **highest epoch**
  wins, so a fenced pod that still believes it is primary (stale epoch) is ignored. Two different pods at the
  same epoch: the route is left alone and a warning is logged.
* Discovery and notifications share one rule (a route is replaced only by a strictly higher epoch), so they can
  run together and discovery can never move clients backwards.
* Kubernetes: `-discover` reads `$GAMESERVER_SETS` (e.g. `arcade-gs-a,arcade-gs-b`), `$GAMESERVER_SERVICE`,
  `$GAMESERVER_HTTP_PORT`, `$POD_NAMESPACE` (the manifest sets them); `-discover-*` flags override.
  Local runs: `-discover-pods 127.0.0.1:9001 -discover-pods 127.0.0.1:9002`.
* With `-discover` you do not need `-default-upstream`.

### Proxy → game pod
* Dials `ws://<primary_addr>/ws?<client query>` (path: `-upstream-path`).
* Adds headers `X-Arcade-Match`, `X-Arcade-Epoch` (**use it to refuse a proxy that is behind**) and `X-Forwarded-For`.
* HTTP status on handshake: `400/401/403` end the player's session (close 1008); anything else (e.g. `503`
  "not primary yet") is retried until `-max-outage`.
* Close codes the pod sends:

| Pod closes with | Proxy does |
|---|---|
| `1000`, `1008`, `1009`, `3000–4999` | player's session is over: same code and reason are passed to the client |
| `1001`, `1011`, `1012`, `1013`, no code, or a dropped TCP connection | "this pod is going away": hold the client, find the next primary |

So a pod stepping down should close with **1012** and then answer new handshakes with **503**.
When a client leaves, the pod sees `1000` (clean) or `1001` (abrupt).

## What the proxy guarantees, and what it cannot

**Guaranteed** (tested): the client's socket stays open through a takeover; client messages sent while there is
no primary are queued (bounded: `-pending-messages`, `-pending-bytes`) and replayed **in order** to the new
primary; once the route changes, nothing more from the old primary reaches the client (the *fence*); stale
epochs are ignored; after `-max-outage` with no primary the client gets close `1013` instead of hanging.

**Not guaranteed — design the game protocol around it:**
1. *In-flight loss.* A message already written to the old primary may never have been applied, or applied
   without its reply getting back. Measured: about one unanswered message per player in the 30-client crash test; 0–12 of ~4,800 across
   20 players in the stepping-down runs. Fix in the game protocol: give every client message an `id`, store
   `last_id` per player in the state snapshot, make handlers idempotent, and have the client (or proxy)
   re-send un-acked messages after a takeover.
2. *Duplicates.* A queued message whose write to the dying primary failed may be replayed after it already
   arrived. Same fix (ids).
3. *Proxy restart drops its clients' sockets.* Run several proxy replicas and drain one at a time
   (`SIGTERM` closes clients with 1001).
4. *No TLS to upstream, no `wss://` client.* Terminate TLS at the ingress/load balancer.
5. The slow-primary case: if the new primary stalls while the proxy is replaying the queue, the next retarget
   waits up to `-write-timeout`.

## Tests

```bash
go vet ./... && go test -race ./...
scripts/e2e.sh crash && scripts/e2e.sh graceful && scripts/e2e.sh discover
python3 scripts/browser_e2e.py          # needs `pip install playwright` + a Chromium
```

Covered: epoch
rules, ordering, fence, outage cap, dial abort, terminal vs non-terminal closes, pings, shutdown, admin API
and a 30-client run through a graceful takeover and through a crash (`pkg/proxy`).

## WebSocket library: `coder/websocket`

The proxy uses `github.com/coder/websocket` v1.8.15 for both sides (`Accept` for clients, `Dial` for game pods).
Choices made on purpose:

* Compression is disabled on both sides (library default, set explicitly).
* The upstream dialer uses its own `http.Client` that ignores `HTTP_PROXY`/`HTTPS_PROXY` and does **not** follow
  redirects, so a pod can never bounce the proxy to some other address. Both are covered by tests.
* `Dial` has no typed handshake error, so the proxy reads the HTTP status from the response it returns
  (`400/401/403` end the session, anything else is retried).
* `Ping` only works while another goroutine is calling `Read`; the proxy always has one.

## Flags

`proxy -h` lists them all. The ones you will touch: `-listen`, `-admin`, `-admin-token`, `-origin`,
`-default-upstream`, `-discover`, `-route match=epoch@host:port`, `-max-outage`, `-pending-messages`, `-upstream-ping`.
