package replication

// Split-brain and fencing validation (Phase 2): a primary is partitioned away,
// the backup promotes itself, and the stale primary later wakes up and keeps
// committing moves. Redis must reject every stale write and stay consistent
// with the promoted primary.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/redis/go-redis/v9"
)

func TestSplitBrainPartitionedPrimaryIsFenced(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	// Primary A boots, claims epoch 1 and replicates a few moves.
	gameA := newTestGame(t, "m1")
	var fencedCalls atomic.Int32
	var fencedWith atomic.Pointer[FencedError]
	repA := NewReplicator(s, ReplicatorConfig{
		MatchID: "m1",
		Epoch:   mustClaim(t, s, "m1", 0),
		OnFenced: func(fe *FencedError) {
			fencedCalls.Add(1)
			fencedWith.Store(fe)
		},
	})
	gameA.RegisterHook(repA.Hook())
	if err := repA.Publish(ctx, gameA.State()); err != nil {
		t.Fatalf("publish initial state: %v", err)
	}
	for i := 0; i < 4; i++ {
		if err := playTurn(gameA); err != nil {
			t.Fatalf("primary A turn %d: %v", i, err)
		}
	}

	// A is partitioned. Backup B observed epoch 1 and takes over.
	epochB, latest, err := s.TakeOver(ctx, "m1", repA.Epoch())
	if err != nil {
		t.Fatalf("TakeOver: %v", err)
	}
	if epochB != repA.Epoch()+1 {
		t.Fatalf("backup epoch = %d, want %d", epochB, repA.Epoch()+1)
	}
	if latest == nil || latest.Sequence != gameA.Sequence() {
		t.Fatalf("backup restored seq %v, want %d", latest, gameA.Sequence())
	}
	gameB, err := latest.Game()
	if err != nil {
		t.Fatalf("restore backup game: %v", err)
	}
	if mustChecksum(t, gameA.State()) != mustChecksum(t, gameB.State()) {
		t.Fatal("backup state differs from primary's last replicated state")
	}
	repB := NewReplicator(s, ReplicatorConfig{MatchID: "m1", Epoch: epochB})
	gameB.RegisterHook(repB.Hook())
	if err := playTurn(gameB); err != nil {
		t.Fatalf("backup B turn: %v", err)
	}
	authoritative := mustChecksum(t, gameB.State())

	// A wakes up and commits several moves, including sequences B has not
	// reached. Each move is applied in A's memory before the hook rejects it,
	// so pick the next legal action from A's current state every time.
	for i := 0; i < 3; i++ {
		var err error
		if p := gameA.CurrentPlayer(); gameA.State().DrewThisTurn {
			_, err = gameA.PassTurn(p)
		} else {
			_, err = gameA.DrawCard(p)
		}
		if !errors.Is(err, uno.ErrHookFailed) || !errors.Is(err, ErrFenced) {
			t.Fatalf("stale primary turn %d err = %v, want ErrHookFailed wrapping ErrFenced", i, err)
		}
	}
	if !repA.Fenced() {
		t.Error("stale primary does not report Fenced()")
	}
	if n := fencedCalls.Load(); n != 1 {
		t.Errorf("OnFenced called %d times, want exactly 1", n)
	}
	if fe := fencedWith.Load(); fe == nil || fe.Held != repA.Epoch() || fe.Current != epochB {
		t.Errorf("OnFenced got %+v, want held %d current %d", fe, repA.Epoch(), epochB)
	}

	// Redis still holds exactly B's state.
	got, err := s.Latest(ctx, "m1")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	st, err := got.State()
	if err != nil {
		t.Fatalf("decode latest: %v", err)
	}
	if got.Epoch != epochB || got.Sequence != gameB.Sequence() || mustChecksum(t, st) != authoritative {
		t.Fatalf("Redis holds epoch %d seq %d, want B's epoch %d seq %d", got.Epoch, got.Sequence, epochB, gameB.Sequence())
	}

	// B keeps replicating normally.
	if err := playTurn(gameB); err != nil {
		t.Fatalf("backup B turn after A's attempts: %v", err)
	}
}

func TestSplitBrainOldEpochRejectedEvenWithHigherSequence(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	old := mustClaim(t, s, "m1", 0)
	if err := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: old, Sequence: 10, Format: FormatJSON, Data: []byte("a10")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	current := mustClaim(t, s, "m1", old)
	if err := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: current, Sequence: 11, Format: FormatJSON, Data: []byte("b11")}); err != nil {
		t.Fatalf("Write as new primary: %v", err)
	}

	err := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: old, Sequence: 1000, Format: FormatJSON, Data: []byte("a1000")})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("stale write err = %v, want ErrFenced", err)
	}
	got, err := s.Latest(ctx, "m1")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if string(got.Data) != "b11" || got.Epoch != current {
		t.Fatalf("Latest = %q epoch %d, want \"b11\" epoch %d", got.Data, got.Epoch, current)
	}
}

func TestSplitBrainDoubleTakeOverOnlyOneWins(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	primary := mustClaim(t, s, "m1", 0)

	// Two backups both observed the same primary epoch and both decide to promote.
	_, _, errX := s.TakeOver(ctx, "m1", primary)
	_, _, errY := s.TakeOver(ctx, "m1", primary)
	if (errX == nil) == (errY == nil) {
		t.Fatalf("both or neither takeover succeeded: %v / %v", errX, errY)
	}
	if !errors.Is(errX, ErrFenced) && !errors.Is(errY, ErrFenced) {
		t.Fatalf("loser did not get ErrFenced: %v / %v", errX, errY)
	}
}

func TestFencedReplicatorFailsFastWithoutRedis(t *testing.T) {
	ctx := context.Background()
	s, rdb := newTestStore(t)
	g := newTestGame(t, "m1")
	r := NewReplicator(s, ReplicatorConfig{MatchID: "m1", Epoch: mustClaim(t, s, "m1", 0)})
	mustClaim(t, s, "m1", r.Epoch()) // someone else promotes

	if err := r.Publish(ctx, g.State()); !errors.Is(err, ErrFenced) {
		t.Fatalf("first publish err = %v, want ErrFenced", err)
	}

	// With the connection gone, a fenced replicator must still answer ErrFenced
	// immediately rather than attempt (and fail) a network write.
	closed := redis.NewClient(&redis.Options{Addr: rdb.(*redis.Client).Options().Addr})
	_ = closed.Close()
	r.store = NewStore(closed)
	if err := r.Publish(ctx, g.State()); !errors.Is(err, ErrFenced) {
		t.Fatalf("publish after fence err = %v, want ErrFenced without contacting Redis", err)
	}
}
