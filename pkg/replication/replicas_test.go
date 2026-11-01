package replication

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// A standalone Redis has no replicas, so WAIT acknowledges nothing: with acks
// required, claims and commits must be reported as not replicated, and fencing
// must still be reported as fencing.
func TestReplicaAcksWithoutReplicas(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t, WithReplicaAcks(1, 20*time.Millisecond))

	if _, err := s.ClaimEpoch(ctx, "m1", 0); !errors.Is(err, ErrNotReplicated) {
		t.Fatalf("ClaimEpoch err = %v, want ErrNotReplicated", err)
	}
	// The claim was applied on the primary even though it was not confirmed.
	if e, err := s.CurrentEpoch(ctx, "m1"); err != nil || e != 1 {
		t.Fatalf("CurrentEpoch = %d, %v; want 1", e, err)
	}
	err := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: 1, Sequence: 1, Format: FormatJSON, Data: []byte("{}")})
	if !errors.Is(err, ErrNotReplicated) {
		t.Fatalf("Write err = %v, want ErrNotReplicated", err)
	}
	if _, err := s.ClaimEpoch(ctx, "m1", 0); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale ClaimEpoch err = %v, want ErrFenced", err)
	}
	err = s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: 0, Sequence: 2, Format: FormatJSON, Data: []byte("{}")})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("stale Write err = %v, want ErrFenced", err)
	}
}

// A freshly promoted replica has an empty script cache; the pipelined path must
// fall back from EVALSHA to EVAL.
func TestReplicaAcksAfterScriptFlush(t *testing.T) {
	ctx := context.Background()
	s, rdb := newTestStore(t, WithReplicaAcks(1, 20*time.Millisecond))
	if err := rdb.ScriptFlush(ctx).Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if _, err := s.ClaimEpoch(ctx, "m1", 0); !errors.Is(err, ErrNotReplicated) {
		t.Fatalf("ClaimEpoch err = %v, want ErrNotReplicated (script ran)", err)
	}
	if e, _ := s.CurrentEpoch(ctx, "m1"); e != 1 {
		t.Fatalf("CurrentEpoch = %d, want 1", e)
	}
}

// TestSentinelFailoverKeepsFencing needs a Sentinel-managed group (master name
// "arcade", at least one replica), e.g. scripts/redis-ha-local.sh:
//
//	REDIS_SENTINEL_ADDRS=127.0.0.1:26390,127.0.0.1:26391,127.0.0.1:26392 go test -run Sentinel ./pkg/replication/
//
// It forces a Redis failover between writes and checks that the promoted Redis
// still holds the epoch, the primary keeps writing, and a stale writer is fenced.
func TestSentinelFailoverKeepsFencing(t *testing.T) {
	addrs := os.Getenv("REDIS_SENTINEL_ADDRS")
	if addrs == "" {
		t.Skip("REDIS_SENTINEL_ADDRS not set")
	}
	ctx := context.Background()
	sentinels := strings.Split(addrs, ",")
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{MasterName: "arcade", SentinelAddrs: sentinels})
	defer func() { _ = rdb.Close() }()
	prefix := fmt.Sprintf("arcadetest-%d", time.Now().UnixNano())
	s := NewStore(rdb, WithKeyPrefix(prefix), WithReplicaAcks(1, 500*time.Millisecond))

	epoch := mustClaim(t, s, "m1", 0)
	write := func(seq int64) error {
		return s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: epoch, Sequence: seq, Format: FormatJSON, Data: []byte("{}")})
	}
	if err := write(1); err != nil {
		t.Fatalf("write before failover: %v", err)
	}

	sentinel := redis.NewSentinelClient(&redis.Options{Addr: sentinels[0]})
	defer func() { _ = sentinel.Close() }()
	// A previous failover (e.g. an earlier run) may still be settling.
	var before []string
	var err error
	for start := time.Now(); ; time.Sleep(time.Second) {
		before, err = sentinel.GetMasterAddrByName(ctx, "arcade").Result()
		if err == nil {
			err = sentinel.Failover(ctx, "arcade").Err()
		}
		if err == nil {
			break
		}
		if !redis.HasErrorPrefix(err, "INPROG") || time.Since(start) > 60*time.Second {
			t.Fatalf("SENTINEL FAILOVER: %v", err)
		}
	}

	// Keep writing through the failover, as the game server would; writes may
	// fail while the old master demotes itself and the client reconnects.
	deadline := time.Now().Add(30 * time.Second)
	var after []string
	for seq := int64(2); ; seq++ {
		if time.Now().After(deadline) {
			t.Fatalf("no successful write on a new master within 30s (last err %v)", err)
		}
		err = write(seq)
		after, _ = sentinel.GetMasterAddrByName(ctx, "arcade").Result()
		if err == nil && strings.Join(after, ":") != strings.Join(before, ":") {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("Redis master moved %v -> %v", before, after)

	if e, err := s.CurrentEpoch(ctx, "m1"); err != nil || e != epoch {
		t.Fatalf("epoch after failover = %d, %v; want %d", e, err, epoch)
	}
	stale := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: epoch - 1, Sequence: 1000, Format: FormatJSON, Data: []byte("{}")})
	if !errors.Is(stale, ErrFenced) {
		t.Fatalf("stale write after failover err = %v, want ErrFenced", stale)
	}

	iter := rdb.Scan(ctx, 0, prefix+":*", 100).Iterator()
	for iter.Next(ctx) {
		rdb.Del(ctx, iter.Val())
	}
}

// While Redis is unavailable (e.g. mid-failover) the replicator keeps retrying
// within its write timeout, so the move commits once Redis is back.
func TestReplicatorRetriesUntilRedisRecovers(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()
	s := NewStore(rdb, WithWriteTimeout(3*time.Second))
	g := newTestGame(t, "m1")
	r := NewReplicator(s, ReplicatorConfig{MatchID: "m1", Epoch: mustClaim(t, s, "m1", 0)})

	mr.SetError("LOADING Redis is loading the dataset in memory")
	time.AfterFunc(300*time.Millisecond, func() { mr.SetError("") })
	start := time.Now()
	if err := r.Publish(ctx, g.State()); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if waited := time.Since(start); waited < 250*time.Millisecond {
		t.Fatalf("Publish returned after %v; expected it to wait for Redis", waited)
	}
	if got, err := s.Latest(ctx, "m1"); err != nil || got.Sequence != g.Sequence() {
		t.Fatalf("Latest = %+v, %v", got, err)
	}

	// Without a recovery the commit gives up at the write timeout.
	mr.SetError("LOADING Redis is loading the dataset in memory")
	short := NewReplicator(s, ReplicatorConfig{MatchID: "m1", Epoch: r.Epoch(), WriteTimeout: 200 * time.Millisecond})
	if err := short.Publish(ctx, g.State()); err == nil {
		t.Fatal("Publish succeeded while Redis was failing")
	}
}
