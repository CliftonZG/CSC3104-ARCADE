package loadgen

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/CSC3104-ARCADE/pkg/uno"
)

func players(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = string(rune('a' + i))
	}
	return out
}

// Endless self-play must keep producing legal moves for a long time without
// ever finishing the match or breaking the 108-card invariants.
func TestEndlessSelfPlayRunsLong(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		g, err := uno.NewGameEngine("m", players(4), seed)
		if err != nil {
			t.Fatal(err)
		}
		p := Policy{Endless: true, Rand: rand.New(rand.NewSource(seed))}
		for i := 0; i < 3000; i++ {
			if _, err := Step(g, p); err != nil {
				t.Fatalf("seed %d move %d: %v", seed, i, err)
			}
		}
		st := g.State()
		if st.Status == uno.StatusFinished {
			t.Fatalf("seed %d: endless match finished", seed)
		}
		if err := st.Validate(); err != nil {
			t.Fatalf("seed %d: invariant broken: %v", seed, err)
		}
		if st.Sequence != 3000 {
			t.Fatalf("seed %d: sequence %d, want 3000", seed, st.Sequence)
		}
	}
}

// Normal (non-endless) self-play must reach a winner.
func TestSelfPlayFinishes(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		g, _ := uno.NewGameEngine("m", players(2+int(seed%3)), seed)
		p := Policy{Rand: rand.New(rand.NewSource(seed))}
		for i := 0; ; i++ {
			_, err := Step(g, p)
			if errors.Is(err, uno.ErrGameOver) {
				break
			}
			if err != nil {
				t.Fatalf("seed %d move %d: %v", seed, i, err)
			}
			if i > 20000 {
				t.Fatalf("seed %d: no winner after 20000 moves", seed)
			}
		}
		if g.Winner() == "" {
			t.Fatalf("seed %d: finished without winner", seed)
		}
	}
}

func TestStepSurfacesHookFailure(t *testing.T) {
	g, _ := uno.NewGameEngine("m", players(3), 7)
	boom := errors.New("redis fenced")
	g.RegisterHook(func(_, _ *uno.State, _ uno.ActionSummary) error { return boom })
	if _, err := Step(g, Policy{}); !errors.Is(err, boom) || !errors.Is(err, uno.ErrHookFailed) {
		t.Fatalf("err = %v, want hook failure", err)
	}
}

func TestDriverStopsOnHookFailureAndContext(t *testing.T) {
	g, _ := uno.NewGameEngine("m", players(4), 3)
	calls := 0
	g.RegisterHook(func(_, _ *uno.State, _ uno.ActionSummary) error {
		calls++
		if calls == 5 {
			return errors.New("fenced")
		}
		return nil
	})
	d := &Driver{Game: g, Interval: time.Millisecond, Policy: Policy{Endless: true}, Logf: t.Logf}
	if err := d.Run(context.Background()); err == nil || calls != 5 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}

	g2, _ := uno.NewGameEngine("m2", players(4), 3)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	d2 := &Driver{Game: g2, Interval: 2 * time.Millisecond, Policy: Policy{Endless: true}}
	if err := d2.Run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if g2.Sequence() == 0 {
		t.Fatal("driver made no moves")
	}
}

func BenchmarkStep(b *testing.B) {
	g, _ := uno.NewGameEngine("m", players(4), 1)
	p := Policy{Endless: true, Rand: rand.New(rand.NewSource(1))}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := Step(g, p); err != nil {
			b.Fatal(err)
		}
	}
}
