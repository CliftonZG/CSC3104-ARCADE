# pkg/replication — Redis snapshot stream & Lua fencing

Owner: Benny. Streams the primary's state to Redis after every valid move and
uses epoch (generation) tokens so a stale primary can never overwrite the state
of a promoted backup.

## Redis layout

All keys for a match share the hash tag `{<matchID>}`, so they live in one
Redis Cluster slot and can be updated atomically by one script.

| Key | Type | Contents |
|---|---|---|
| `arcade:match:{id}:epoch` | string | Current fencing epoch. `0`/missing = never claimed. |
| `arcade:match:{id}:snapshot` | hash | Latest snapshot: `data`, `format` (`json`/`binary`), `seq`, `epoch`, `ts_ms` (Redis server time). |
| `arcade:match:{id}:stream` | stream | Capped (~256) history of committed snapshots; same fields. |

## Fencing protocol

1. A pod becomes primary by **claiming** the next epoch:
   `ClaimEpoch(matchID, expected)` does `if epoch == expected { INCR }` in Lua
   (`scripts/claim.lua`). Two pods racing from the same observed epoch → exactly one wins.
2. Every snapshot write carries the writer's epoch. `scripts/commit.lua`
   atomically checks `writer_epoch == current_epoch` and `seq > stored_seq`
   before writing. Otherwise the write is rejected (`ErrFenced` / `ErrStaleSequence`).
   Retrying an already-committed (epoch, seq) is a no-op.
3. Once a newer epoch exists, the old primary's writes all fail with `ErrFenced`,
   and its `Replicator` calls `OnFenced` once, then fails fast without touching Redis.

Redis must persist the epoch key (AOF/RDB). If Redis loses the epoch, all
writers are fenced (safe, but the match stalls until a pod re-claims).

## API for teammates

**Primary pod (gameserver)**: see `cmd/gameserver/replication.go`.
```go
store := replication.NewStore(redis.NewClient(&redis.Options{Addr: addr}))
epoch, latest, err := store.TakeOver(ctx, matchID, observedEpoch) // claim, then read latest
rep := replication.NewReplicator(store, replication.ReplicatorConfig{
    MatchID: matchID, Epoch: epoch, OnFenced: stepDown,
})
engine.RegisterHook(rep.Hook())  // commits every valid move synchronously
rep.Publish(ctx, engine.State()) // persist the starting state (fresh match)
```

**Failure detector / promotion (Marcus)**:
- `store.SnapshotAge(ctx, matchID)`: time since the last commit, measured on
  the Redis clock (no pod clock skew). This is the "Redis snapshot silence" signal.
- `store.Follow(ctx, matchID, "$", fn)`: push-style tail of new snapshots
  (keep a hot standby copy and track `Snapshot.Epoch`).
- `store.TakeOver(ctx, matchID, lastSeenEpoch)`: the promotion step. It claims
  first, then loads the latest snapshot; `latest.Game()` returns a ready engine.
  A `*FencedError` means someone else already promoted, so don't serve.

**Engine (Clifton)**: `Game.ApplyAction` swaps in the new state *before* hooks
run, so a fenced move is already applied in the stale primary's memory. The pod
must stop serving on `OnFenced` (the gameserver shuts down). `ApplyAction`'s
error wraps `replication.ErrFenced` (`errors.Is` works).

## High availability: Redis Sentinel

A single Redis is a single point of failure, so production runs a Sentinel
group: one Redis primary, replicas, and three Sentinels that promote a replica
when the primary dies. The gameserver connects through the Sentinels and
follows the new primary automatically:

```bash
scripts/redis-ha-local.sh up        # primary :6390, replicas :6391 :6392, sentinels :26390-26392
go run ./cmd/gameserver -role primary -match m1 -autoplay 500ms   -redis-sentinels 127.0.0.1:26390,127.0.0.1:26391,127.0.0.1:26392 -redis-acks 1
scripts/redis-ha-local.sh kill      # kill -9 the Redis primary; the game pauses ~2s, then continues
```

| Flag / env | Meaning |
|---|---|
| `-redis-sentinels` / `REDIS_SENTINELS` | Sentinel addresses (overrides `-redis`) |
| `-redis-master` / `REDIS_MASTER_NAME` | Sentinel master name (default `arcade`) |
| `-redis-acks` / `REDIS_MIN_REPLICA_ACKS` | replicas that must confirm every claim and commit (`WAIT`) |
| `-redis-ack-timeout` | how long to wait for them (default 250ms) |
| `-redis-write-timeout` | how long a move's commit is retried (default 3s with Sentinel, else 500ms) |

**Why the acks matter for fencing.** Redis replicates asynchronously. Without
`WAIT`, Sentinel could promote a replica that never received the latest
`claim.lua`: the epoch would go backwards and a fenced primary could write
again. With `WithReplicaAcks(1, ...)` a claim or commit only succeeds once a
replica has it, and Sentinel promotes the most up-to-date replica, so the
epoch and every acknowledged move survive a Redis failover. Without enough
replicas it fails with `ErrNotReplicated` (consistency over availability);
with two replicas and `acks=1`, one replica can be down. The Redis nodes also
run `min-replicas-to-write 1`, so an old Redis primary cut off from its
replicas stops accepting writes.

The Replicator retries transient errors (failover in progress, replica not yet
caught up) until its write timeout; retries are idempotent (`DUP`). Fencing and
stale sequences are never retried.

**WSL note:** WSL2's default `tsc` clock can jump backwards, which puts Sentinel
into TILT mode (no failovers for 30s). Fix until the next WSL restart:
`wsl -u root -e sh -c "echo hyperv_clocksource_tsc_page > /sys/devices/system/clocksource/clocksource0/current_clocksource"`

## Tests

```bash
go test ./pkg/replication/                                   # in-memory Redis (miniredis)
REDIS_TEST_ADDR=localhost:6379 go test ./pkg/replication/    # real Redis 7+ (standalone)
REDIS_SENTINEL_ADDRS=127.0.0.1:26390,127.0.0.1:26391,127.0.0.1:26392   go test -run Sentinel ./pkg/replication/                   # forces a real Sentinel failover
```
`fencing_test.go` holds the Phase 2 split-brain validation: partitioned
primary wakes up and keeps committing, higher-seq stale writes, double takeover,
and fail-fast after fencing.
