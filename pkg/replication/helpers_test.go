package replication

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

var testPlayers = []string{"p1", "p2", "p3", "p4"}

// newTestStore returns a Store backed by miniredis, or by the real Redis at
// REDIS_TEST_ADDR when set (keys are namespaced per test and removed after).
func newTestStore(t *testing.T, opts ...Option) (*Store, redis.UniversalClient) {
	t.Helper()
	addr := os.Getenv("REDIS_TEST_ADDR")
	prefix := DefaultKeyPrefix
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	} else {
		prefix = fmt.Sprintf("arcadetest-%d", time.Now().UnixNano())
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+":*", 100).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
		_ = rdb.Close()
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("redis unavailable at %s: %v", addr, err)
	}
	return NewStore(rdb, append([]Option{WithKeyPrefix(prefix)}, opts...)...), rdb
}

func newTestGame(t *testing.T, matchID string) *uno.Game {
	t.Helper()
	g, err := uno.NewGameEngine(matchID, testPlayers, 7)
	if err != nil {
		t.Fatalf("NewGameEngine: %v", err)
	}
	return g
}

// playTurn advances the game by one legal turn: the current player draws and,
// if the drawn card was playable (turn not auto-advanced), passes.
func playTurn(g *uno.Game) error {
	p := g.CurrentPlayer()
	if _, err := g.DrawCard(p); err != nil {
		return err
	}
	if g.CurrentPlayer() == p {
		_, err := g.PassTurn(p)
		return err
	}
	return nil
}

func mustChecksum(t *testing.T, st *uno.State) string {
	t.Helper()
	sum, err := st.Checksum()
	if err != nil {
		t.Fatalf("Checksum: %v", err)
	}
	return sum
}

func mustClaim(t *testing.T, s *Store, matchID string, expected int64) int64 {
	t.Helper()
	epoch, err := s.ClaimEpoch(context.Background(), matchID, expected)
	if err != nil {
		t.Fatalf("ClaimEpoch(%d): %v", expected, err)
	}
	return epoch
}
