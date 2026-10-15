package uno

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func TestEngineLifecycle(t *testing.T) {
	eng, err := NewGameEngine("eng-match-1", []string{"p1", "p2"}, 100)
	if err != nil {
		t.Fatalf("NewGameEngine failed: %v", err)
	}

	if eng.MatchID() != "eng-match-1" {
		t.Errorf("MatchID = %q, want eng-match-1", eng.MatchID())
	}
	if eng.Sequence() != 0 {
		t.Errorf("Initial sequence = %d, want 0", eng.Sequence())
	}
	if eng.IsFinished() {
		t.Error("Initial game should not be finished")
	}

	top, ok := eng.TopCard()
	if !ok {
		t.Fatal("TopCard returned false on valid initial state")
	}
	if top.Colour == "" || top.Value == "" {
		t.Errorf("Invalid top card: %v", top)
	}
}

func TestEngineApplyActionTransactional(t *testing.T) {
	eng, err := NewGameEngine("tx-match", []string{"alice", "bob"}, 200)
	if err != nil {
		t.Fatalf("NewGameEngine failed: %v", err)
	}

	initialSeq := eng.Sequence()
	initialPlayer := eng.CurrentPlayer()

	// 1. Attempt move from wrong player (out of turn)
	wrongPlayer := "bob"
	if initialPlayer == "bob" {
		wrongPlayer = "alice"
	}
	_, err = eng.ApplyAction(Action{
		Type:      ActionPlay,
		PlayerID:  wrongPlayer,
		CardIndex: 0,
	})
	if err == nil || !errors.Is(err, ErrNotYourTurn) {
		t.Fatalf("expected ErrNotYourTurn, got %v", err)
	}

	// Verify state completely unmodified
	if eng.Sequence() != initialSeq {
		t.Errorf("Sequence changed after failed move: got %d, want %d", eng.Sequence(), initialSeq)
	}
	if eng.CurrentPlayer() != initialPlayer {
		t.Errorf("CurrentPlayer changed after failed move: got %q, want %q", eng.CurrentPlayer(), initialPlayer)
	}

	// 2. Attempt invalid card index
	_, err = eng.ApplyAction(Action{
		Type:      ActionPlay,
		PlayerID:  initialPlayer,
		CardIndex: 999,
	})
	if err == nil || !errors.Is(err, ErrCardIndex) {
		t.Fatalf("expected ErrCardIndex, got %v", err)
	}
	if eng.Sequence() != initialSeq {
		t.Errorf("Sequence changed after out-of-bounds play: got %d, want %d", eng.Sequence(), initialSeq)
	}
}

func TestEngineMoveHooks(t *testing.T) {
	eng, err := NewGameEngine("hook-match", []string{"p1", "p2"}, 300)
	if err != nil {
		t.Fatalf("NewGameEngine failed: %v", err)
	}

	var hookCallCount int64
	var lastSummary ActionSummary

	eng.RegisterHook(func(prev *State, next *State, summary ActionSummary) error {
		atomic.AddInt64(&hookCallCount, 1)
		lastSummary = summary
		if next.Sequence <= prev.Sequence {
			t.Errorf("Hook received non-monotonic sequence: prev=%d, next=%d", prev.Sequence, next.Sequence)
		}
		return nil
	})

	cur := eng.CurrentPlayer()
	// Execute a valid draw
	nextState, err := eng.DrawCard(cur)
	if err != nil {
		t.Fatalf("DrawCard failed: %v", err)
	}

	if atomic.LoadInt64(&hookCallCount) != 1 {
		t.Fatalf("expected 1 hook invocation, got %d", atomic.LoadInt64(&hookCallCount))
	}
	if lastSummary.Action.Type != ActionDraw {
		t.Errorf("Summary action type = %q, want draw", lastSummary.Action.Type)
	}
	if lastSummary.Sequence != nextState.Sequence {
		t.Errorf("Summary sequence = %d, want %d", lastSummary.Sequence, nextState.Sequence)
	}

	// Test failing hook stops execution and surfaces error
	eng.RegisterHook(func(prev *State, next *State, summary ActionSummary) error {
		return fmt.Errorf("disk full simulation")
	})

	newCur := eng.CurrentPlayer()
	_, err = eng.DrawCard(newCur)
	if err == nil || !errors.Is(err, ErrHookFailed) {
		t.Fatalf("expected ErrHookFailed, got %v", err)
	}
}

func TestEngineApplySnapshotHotFailover(t *testing.T) {
	primary, err := NewGameEngine("failover-match", []string{"nodeA", "nodeB", "nodeC"}, 400)
	if err != nil {
		t.Fatalf("primary NewGameEngine failed: %v", err)
	}

	// Play 8 turns on Primary pod
	for i := 0; i < 8; i++ {
		cur := primary.CurrentPlayer()
		hand := primary.State().Hands[cur]
		played := false
		for idx, card := range hand {
			if IsPlayable(primary.State(), cur, card) {
				var col Colour
				if card.IsWild() {
					col = ColourGreen
				}
				if _, playErr := primary.PlayCard(cur, idx, col); playErr == nil {
					played = true
					break
				}
			}
		}
		if !played {
			if _, drawErr := primary.DrawCard(cur); drawErr != nil {
				t.Fatalf("turn %d: draw failed: %v", i, drawErr)
			}
		}
	}

	// Primary exports snapshot before crashing
	snapBytes, err := primary.ExportSnapshot()
	if err != nil {
		t.Fatalf("ExportSnapshot failed: %v", err)
	}

	// Standby pod starts cold or uninitialized, then hot-applies snapshot
	standby, err := RestoreGameFromSnapshot(snapBytes)
	if err != nil {
		t.Fatalf("Standby RestoreGameFromSnapshot failed: %v", err)
	}

	// Verify exact sequence, turn, and checksum parity
	if standby.Sequence() != primary.Sequence() {
		t.Fatalf("Sequence mismatch: standby=%d, primary=%d", standby.Sequence(), primary.Sequence())
	}
	if standby.CurrentPlayer() != primary.CurrentPlayer() {
		t.Fatalf("CurrentPlayer mismatch: standby=%q, primary=%q", standby.CurrentPlayer(), primary.CurrentPlayer())
	}

	pChk, _ := primary.Checksum()
	sChk, _ := standby.Checksum()
	if pChk != sChk {
		t.Fatalf("SHA-256 Checksum mismatch after failover: %s != %s", pChk, sChk)
	}

	// Standby pod continues the game without a glitch
	cur := standby.CurrentPlayer()
	_, err = standby.DrawCard(cur)
	if err != nil {
		t.Fatalf("Standby resumed turn failed: %v", err)
	}
	if standby.Sequence() != primary.Sequence()+1 {
		t.Errorf("Standby sequence failed to increment: got %d, want %d", standby.Sequence(), primary.Sequence()+1)
	}
}

func TestEngineConcurrencyRaceSafety(t *testing.T) {
	eng, err := NewGameEngine("race-match", []string{"p1", "p2"}, 500)
	if err != nil {
		t.Fatalf("NewGameEngine failed: %v", err)
	}

	var wg sync.WaitGroup
	workers := 20
	iterations := 50

	// Concurrent readers reading state and exporting snapshots
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_ = eng.State()
				_ = eng.Sequence()
				_ = eng.CurrentPlayer()
				_, _ = eng.ExportSnapshot()
				_, _ = eng.Checksum()
			}
		}()
	}

	// Serial turn player
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < iterations; j++ {
			cur := eng.CurrentPlayer()
			_, _ = eng.DrawCard(cur)
		}
	}()

	wg.Wait()
}
