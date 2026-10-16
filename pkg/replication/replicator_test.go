package replication

import (
	"context"
	"testing"
)

func TestReplicatorHookPersistsEveryMove(t *testing.T) {
	for _, format := range []Format{FormatJSON, FormatBinary} {
		t.Run(string(format), func(t *testing.T) {
			ctx := context.Background()
			s, _ := newTestStore(t)
			g := newTestGame(t, "m1")
			r := NewReplicator(s, ReplicatorConfig{MatchID: "m1", Epoch: mustClaim(t, s, "m1", 0), Format: format})
			g.RegisterHook(r.Hook())
			if err := r.Publish(ctx, g.State()); err != nil {
				t.Fatalf("publish initial state: %v", err)
			}

			for i := 0; i < 30 && !g.IsFinished(); i++ {
				if err := playTurn(g); err != nil {
					t.Fatalf("turn %d: %v", i, err)
				}
				got, err := s.Latest(ctx, "m1")
				if err != nil {
					t.Fatalf("Latest after turn %d: %v", i, err)
				}
				if got.Sequence != g.Sequence() || got.Format != format {
					t.Fatalf("turn %d: Redis seq %d format %s, engine seq %d format %s",
						i, got.Sequence, got.Format, g.Sequence(), format)
				}
				st, err := got.State()
				if err != nil {
					t.Fatalf("decode after turn %d: %v", i, err)
				}
				if mustChecksum(t, st) != mustChecksum(t, g.State()) {
					t.Fatalf("turn %d: replicated state differs from engine state", i)
				}
			}
		})
	}
}
