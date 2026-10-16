package replication

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestWriteLatestRoundTrip(t *testing.T) {
	for _, format := range []Format{FormatJSON, FormatBinary} {
		t.Run(string(format), func(t *testing.T) {
			ctx := context.Background()
			s, _ := newTestStore(t)
			g := newTestGame(t, "m1")
			for i := 0; i < 5; i++ {
				if err := playTurn(g); err != nil {
					t.Fatalf("playTurn: %v", err)
				}
			}
			st := g.State()
			data, err := format.Encode(st)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			epoch := mustClaim(t, s, "m1", 0)
			snap := &Snapshot{MatchID: "m1", Epoch: epoch, Sequence: st.Sequence, Format: format, Data: data}
			if err := s.Write(ctx, snap); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if snap.WrittenAt.IsZero() {
				t.Error("WrittenAt not set after Write")
			}

			got, err := s.Latest(ctx, "m1")
			if err != nil {
				t.Fatalf("Latest: %v", err)
			}
			if got.Epoch != epoch || got.Sequence != st.Sequence || got.Format != format {
				t.Errorf("Latest metadata = epoch %d seq %d format %s, want %d %d %s",
					got.Epoch, got.Sequence, got.Format, epoch, st.Sequence, format)
			}
			if !got.WrittenAt.Equal(snap.WrittenAt) {
				t.Errorf("WrittenAt = %v, want %v", got.WrittenAt, snap.WrittenAt)
			}
			restored, err := got.Game()
			if err != nil {
				t.Fatalf("Game: %v", err)
			}
			if a, b := mustChecksum(t, st), mustChecksum(t, restored.State()); a != b {
				t.Errorf("restored checksum %s, want %s", b, a)
			}
		})
	}
}

func TestLatestNoSnapshot(t *testing.T) {
	s, _ := newTestStore(t)
	if _, err := s.Latest(context.Background(), "missing"); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("Latest err = %v, want ErrNoSnapshot", err)
	}
}

func TestWriteRequiresClaimedEpoch(t *testing.T) {
	s, _ := newTestStore(t)
	err := s.Write(context.Background(), &Snapshot{MatchID: "m1", Epoch: 1, Sequence: 0, Format: FormatJSON, Data: []byte("{}")})
	var fe *FencedError
	if !errors.As(err, &fe) || !errors.Is(err, ErrFenced) {
		t.Fatalf("err = %v, want FencedError", err)
	}
	if fe.Held != 1 || fe.Current != 0 {
		t.Errorf("FencedError = held %d current %d, want 1 0", fe.Held, fe.Current)
	}
}

func TestWriteRejectsFutureEpoch(t *testing.T) {
	s, _ := newTestStore(t)
	epoch := mustClaim(t, s, "m1", 0)
	err := s.Write(context.Background(), &Snapshot{MatchID: "m1", Epoch: epoch + 1, Format: FormatJSON, Data: []byte("{}")})
	if !errors.Is(err, ErrFenced) {
		t.Fatalf("err = %v, want ErrFenced", err)
	}
}

func TestWriteSequenceOrdering(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	epoch := mustClaim(t, s, "m1", 0)
	write := func(seq int64, data string) error {
		return s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: epoch, Sequence: seq, Format: FormatJSON, Data: []byte(data)})
	}

	if err := write(5, "five"); err != nil {
		t.Fatalf("write seq 5: %v", err)
	}
	if err := write(5, "five"); err != nil {
		t.Errorf("retry of seq 5 should be idempotent, got %v", err)
	}
	if err := write(4, "four"); !errors.Is(err, ErrStaleSequence) {
		t.Errorf("write seq 4 err = %v, want ErrStaleSequence", err)
	}
	if err := write(6, "six"); err != nil {
		t.Fatalf("write seq 6: %v", err)
	}

	got, err := s.Latest(ctx, "m1")
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if got.Sequence != 6 || string(got.Data) != "six" {
		t.Errorf("Latest = seq %d %q, want 6 \"six\"", got.Sequence, got.Data)
	}
}

func TestClaimEpochCompareAndIncrement(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)

	if e := mustClaim(t, s, "m1", 0); e != 1 {
		t.Fatalf("first claim = %d, want 1", e)
	}
	_, err := s.ClaimEpoch(ctx, "m1", 0)
	var fe *FencedError
	if !errors.As(err, &fe) || fe.Current != 1 {
		t.Fatalf("claim from stale epoch err = %v, want FencedError current 1", err)
	}
	if e := mustClaim(t, s, "m1", 1); e != 2 {
		t.Fatalf("second claim = %d, want 2", e)
	}
	if e, err := s.CurrentEpoch(ctx, "m1"); err != nil || e != 2 {
		t.Fatalf("CurrentEpoch = %d, %v; want 2", e, err)
	}
	if e, err := s.CurrentEpoch(ctx, "other"); err != nil || e != 0 {
		t.Fatalf("CurrentEpoch(unclaimed) = %d, %v; want 0", e, err)
	}
}

func TestClaimEpochConcurrentSingleWinner(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	mustClaim(t, s, "m1", 0)

	const contenders = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins, fenced := 0, 0
	for i := 0; i < contenders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.ClaimEpoch(ctx, "m1", 1)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				wins++
			case errors.Is(err, ErrFenced):
				fenced++
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if wins != 1 || fenced != contenders-1 {
		t.Fatalf("wins=%d fenced=%d, want 1 and %d", wins, fenced, contenders-1)
	}
}

func TestTakeOverWithoutSnapshot(t *testing.T) {
	s, _ := newTestStore(t)
	epoch, latest, err := s.TakeOver(context.Background(), "m1", 0)
	if err != nil || epoch != 1 || latest != nil {
		t.Fatalf("TakeOver = %d, %v, %v; want 1, nil, nil", epoch, latest, err)
	}
}

func TestSnapshotAge(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestStore(t)
	if _, err := s.SnapshotAge(ctx, "m1"); !errors.Is(err, ErrNoSnapshot) {
		t.Fatalf("SnapshotAge before write err = %v, want ErrNoSnapshot", err)
	}

	epoch := mustClaim(t, s, "m1", 0)
	if err := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: epoch, Format: FormatJSON, Data: []byte("{}")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	age, err := s.SnapshotAge(ctx, "m1")
	if err != nil {
		t.Fatalf("SnapshotAge: %v", err)
	}
	if age < 0 || age > 5*time.Second {
		t.Fatalf("SnapshotAge = %v, want within [0, 5s]", age)
	}
}

func TestFollowDeliversStreamInOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, _ := newTestStore(t)
	g := newTestGame(t, "m1")
	r := NewReplicator(s, ReplicatorConfig{MatchID: "m1", Epoch: mustClaim(t, s, "m1", 0)})
	g.RegisterHook(r.Hook())

	const turns = 5
	for i := 0; i < turns; i++ {
		if err := playTurn(g); err != nil {
			t.Fatalf("playTurn: %v", err)
		}
	}

	var seqs []int64
	errDone := errors.New("done")
	err := s.Follow(ctx, "m1", "0", func(snap *Snapshot) error {
		if _, err := snap.State(); err != nil {
			t.Errorf("stream snapshot seq %d does not decode: %v", snap.Sequence, err)
		}
		seqs = append(seqs, snap.Sequence)
		if snap.Sequence == g.Sequence() {
			return errDone
		}
		return nil
	})
	if !errors.Is(err, errDone) {
		t.Fatalf("Follow returned %v, want errDone (seqs so far %v)", err, seqs)
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("stream sequences not increasing: %v", seqs)
		}
	}
}

func TestFollowStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s, _ := newTestStore(t)
	err := s.Follow(ctx, "m1", "$", func(*Snapshot) error { return nil })
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Follow err = %v, want context.DeadlineExceeded", err)
	}
}

func TestStreamDisabled(t *testing.T) {
	ctx := context.Background()
	s, rdb := newTestStore(t, WithStreamMaxLen(0))
	epoch := mustClaim(t, s, "m1", 0)
	if err := s.Write(ctx, &Snapshot{MatchID: "m1", Epoch: epoch, Format: FormatJSON, Data: []byte("{}")}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n, err := rdb.Exists(ctx, s.streamKey("m1")).Result(); err != nil || n != 0 {
		t.Fatalf("stream key exists=%d err=%v, want stream disabled", n, err)
	}
}
