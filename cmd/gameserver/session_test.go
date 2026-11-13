package main

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/uno"
)

func TestGameSessionLifecycleAndHooks(t *testing.T) {
	game1, err := uno.NewGameEngine("test-match", []string{"p1", "p2", "p3", "p4"}, 42)
	if err != nil {
		t.Fatalf("failed to create game1: %v", err)
	}

	session := NewGameSession(game1)

	var hookCallCount atomic.Int32
	session.RegisterHook(func(prev, next *uno.State, summary uno.ActionSummary) error {
		hookCallCount.Add(1)
		return nil
	})

	// Apply a valid draw action
	curPlayer := session.State().CurrentPlayer()
	_, err = session.ApplyAction(uno.Action{
		Type:     uno.ActionDraw,
		PlayerID: curPlayer,
	})
	if err != nil {
		t.Fatalf("ApplyAction failed: %v", err)
	}

	if hookCallCount.Load() != 1 {
		t.Fatalf("expected hook to be called 1 time, got %d", hookCallCount.Load())
	}

	// Verify sequence number advanced
	if session.Sequence() != 1 {
		t.Fatalf("expected sequence 1, got %d", session.Sequence())
	}

	// Simulate standby takeover: new game restored from snapshot
	game2, err := uno.NewGameEngine("test-match", []string{"p1", "p2", "p3", "p4"}, 99)
	if err != nil {
		t.Fatalf("failed to create game2: %v", err)
	}

	// Swap game into session
	session.SwapGame(game2)

	// Apply another action on the new game instance
	curPlayer2 := session.State().CurrentPlayer()
	_, err = session.ApplyAction(uno.Action{
		Type:     uno.ActionDraw,
		PlayerID: curPlayer2,
	})
	if err != nil {
		t.Fatalf("ApplyAction on swapped game failed: %v", err)
	}

	// The persistent hook MUST have been re-attached and invoked again!
	if hookCallCount.Load() != 2 {
		t.Fatalf("expected hook to be preserved across SwapGame and called 2 times, got %d", hookCallCount.Load())
	}
}

func TestGameSessionConcurrency(t *testing.T) {
	game, err := uno.NewGameEngine("concurrent-match", []string{"p1", "p2"}, 100)
	if err != nil {
		t.Fatalf("failed to create game: %v", err)
	}

	session := NewGameSession(game)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = session.State()
			_ = session.Sequence()
			_ = session.MatchID()
			_, _ = session.ExportSnapshot()
			_, _ = session.ExportBinary()
		}()
	}
	wg.Wait()
}

func TestGameServerWSMultiRoomAndReset(t *testing.T) {
	game, err := uno.NewGameEngine("room0", []string{"player1", "player2", "player3", "player4"}, 42)
	if err != nil {
		t.Fatalf("failed to create game: %v", err)
	}
	status := fleet.NewStatus(fleet.Identity{MatchID: "room0"}, fleet.RolePrimary)
	ws := NewGameServerWS(status, NewGameSession(game))

	s0 := ws.getOrCreateSession("room0")
	s1 := ws.getOrCreateSession("room1")
	s2 := ws.getOrCreateSession("room2")

	if s0.MatchID() != "room0" || s1.MatchID() != "room1" || s2.MatchID() != "room2" {
		t.Fatalf("unexpected match IDs: %s, %s, %s", s0.MatchID(), s1.MatchID(), s2.MatchID())
	}

	// Advance room1 independently from room0
	if _, err := s1.ApplyAction(uno.Action{Type: uno.ActionDraw, PlayerID: s1.State().CurrentPlayer()}); err != nil {
		t.Fatalf("draw in room1: %v", err)
	}
	if s1.Sequence() != 1 || s0.Sequence() != 0 || s2.Sequence() != 0 {
		t.Fatalf("rooms were not isolated: seqs=(%d, %d, %d)", s0.Sequence(), s1.Sequence(), s2.Sequence())
	}

	// Reset room1 and verify sequence returns to 0
	ws.resetSession("room1")
	if ws.getOrCreateSession("room1").Sequence() != 0 {
		t.Fatalf("expected room1 sequence 0 after reset, got %d", ws.getOrCreateSession("room1").Sequence())
	}
}
