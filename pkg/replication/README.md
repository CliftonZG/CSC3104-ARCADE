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

## Tests

```bash
go test ./pkg/replication/                                   # in-memory Redis (miniredis)
REDIS_TEST_ADDR=localhost:6379 go test ./pkg/replication/    # real Redis 7+
```
`fencing_test.go` holds the Phase 2 split-brain validation: partitioned
primary wakes up and keeps committing, higher-seq stale writes, double takeover,
and fail-fast after fencing.
