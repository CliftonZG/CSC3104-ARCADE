package uno

import (
	"bytes"
	"strings"
	"testing"
)

func TestSnapshotExportAndRestoreRoundTrip(t *testing.T) {
	s, err := NewGame("match-101", []string{"p1", "p2", "p3"}, 42)
	if err != nil {
		t.Fatalf("NewGame failed: %v", err)
	}

	// Play a few turns to advance state
	for i := 0; i < 5; i++ {
		cur := s.CurrentPlayer()
		hand := s.Hands[cur]
		played := false
		for idx, card := range hand {
			if IsPlayable(s, cur, card) {
				var col Colour
				if card.IsWild() {
					col = ColourRed
				}
				next, playErr := Play(s, cur, idx, col)
				if playErr == nil {
					s = next
					played = true
					break
				}
			}
		}
		if !played {
			next, drawErr := Draw(s, cur)
			if drawErr != nil {
				t.Fatalf("turn %d Draw failed: %v", i, drawErr)
			}
			s = next
		}
	}

	snap, err := s.ExportSnapshot()
	if err != nil {
		t.Fatalf("ExportSnapshot failed: %v", err)
	}
	if len(snap) == 0 {
		t.Fatal("ExportSnapshot produced empty byte slice")
	}

	restored, err := RestoreFromSnapshot(snap)
	if err != nil {
		t.Fatalf("RestoreFromSnapshot failed: %v", err)
	}

	if restored.MatchID != s.MatchID {
		t.Errorf("MatchID mismatch: got %q, want %q", restored.MatchID, s.MatchID)
	}
	if restored.Sequence != s.Sequence {
		t.Errorf("Sequence mismatch: got %d, want %d", restored.Sequence, s.Sequence)
	}
	if restored.Status != s.Status {
		t.Errorf("Status mismatch: got %q, want %q", restored.Status, s.Status)
	}
	if restored.CurrentPlayerIndex != s.CurrentPlayerIndex {
		t.Errorf("CurrentPlayerIndex mismatch: got %d, want %d", restored.CurrentPlayerIndex, s.CurrentPlayerIndex)
	}
	if restored.CurrentColour != s.CurrentColour {
		t.Errorf("CurrentColour mismatch: got %q, want %q", restored.CurrentColour, s.CurrentColour)
	}
	if len(restored.Deck) != len(s.Deck) {
		t.Errorf("Deck length mismatch: got %d, want %d", len(restored.Deck), len(s.Deck))
	}
	if len(restored.DiscardPile) != len(s.DiscardPile) {
		t.Errorf("DiscardPile length mismatch: got %d, want %d", len(restored.DiscardPile), len(s.DiscardPile))
	}
}

func TestSnapshotDeterministicBitForBit(t *testing.T) {
	// Two completely separate games created with the exact same parameters
	g1, err := NewGame("match-deterministic", []string{"alice", "bob", "carol", "dave"}, 9999)
	if err != nil {
		t.Fatalf("NewGame 1 failed: %v", err)
	}
	g2, err := NewGame("match-deterministic", []string{"alice", "bob", "carol", "dave"}, 9999)
	if err != nil {
		t.Fatalf("NewGame 2 failed: %v", err)
	}

	snap1, err := g1.ExportSnapshot()
	if err != nil {
		t.Fatalf("g1.ExportSnapshot failed: %v", err)
	}
	snap2, err := g2.ExportSnapshot()
	if err != nil {
		t.Fatalf("g2.ExportSnapshot failed: %v", err)
	}

	if !bytes.Equal(snap1, snap2) {
		t.Fatal("Snapshots from identical games are NOT bit-for-bit equal")
	}

	chk1, err := g1.Checksum()
	if err != nil {
		t.Fatalf("g1.Checksum failed: %v", err)
	}
	chk2, err := g2.Checksum()
	if err != nil {
		t.Fatalf("g2.Checksum failed: %v", err)
	}

	if chk1 != chk2 {
		t.Fatalf("Checksums mismatch: %s != %s", chk1, chk2)
	}
}

func TestSnapshotStreamingWriterAndReader(t *testing.T) {
	s, err := NewGame("stream-test", []string{"p1", "p2"}, 1234)
	if err != nil {
		t.Fatalf("NewGame failed: %v", err)
	}

	var pipe bytes.Buffer
	if err := s.ExportSnapshotToWriter(&pipe); err != nil {
		t.Fatalf("ExportSnapshotToWriter failed: %v", err)
	}

	restored, err := RestoreFromReader(&pipe)
	if err != nil {
		t.Fatalf("RestoreFromReader failed: %v", err)
	}

	if restored.MatchID != s.MatchID {
		t.Errorf("Restored match ID mismatch: got %q, want %q", restored.MatchID, s.MatchID)
	}
	if err := restored.Validate(); err != nil {
		t.Fatalf("Restored state failed validation: %v", err)
	}
}

func TestSnapshotCardConservationLawThroughoutMatch(t *testing.T) {
	s, err := NewGame("conservation-match", []string{"p1", "p2", "p3", "p4"}, 777)
	if err != nil {
		t.Fatalf("NewGame failed: %v", err)
	}

	if err := s.Validate(); err != nil {
		t.Fatalf("Initial state invalid: %v", err)
	}

	for turn := 0; turn < 150 && s.Status == StatusInProgress; turn++ {
		cur := s.CurrentPlayer()
		hand := s.Hands[cur]
		acted := false

		for idx, card := range hand {
			if IsPlayable(s, cur, card) {
				var col Colour
				if card.IsWild() {
					col = ColourYellow
				}
				next, playErr := Play(s, cur, idx, col)
				if playErr == nil {
					s = next
					acted = true
					break
				}
			}
		}

		if !acted {
			next, drawErr := Draw(s, cur)
			if drawErr != nil {
				t.Fatalf("turn %d: unexpected draw error: %v", turn, drawErr)
			}
			s = next
		}

		// Verify 108 cards invariant after every move
		if err := s.Validate(); err != nil {
			t.Fatalf("turn %d state invariant violated: %v", turn, err)
		}
	}
}

func TestSnapshotCorruptionResistance(t *testing.T) {
	t.Run("empty payload", func(t *testing.T) {
		_, err := RestoreFromSnapshot(nil)
		if err == nil || !strings.Contains(err.Error(), "invalid snapshot") {
			t.Fatalf("expected ErrInvalidSnapshot on empty, got %v", err)
		}
	})

	t.Run("truncated json", func(t *testing.T) {
		_, err := RestoreFromSnapshot([]byte(`{"match_id":"m1",`))
		if err == nil || !strings.Contains(err.Error(), "invalid snapshot") {
			t.Fatalf("expected unmarshal error, got %v", err)
		}
	})

	t.Run("missing hand entry", func(t *testing.T) {
		s, _ := NewGame("m1", []string{"p1", "p2"}, 1)
		delete(s.Hands, "p2")
		raw, _ := s.ExportSnapshot()
		_, err := RestoreFromSnapshot(raw)
		if err == nil {
			t.Fatal("expected validation failure for missing hand")
		}
	})

	t.Run("card duplication violating 108 count", func(t *testing.T) {
		s, _ := NewGame("m1", []string{"p1", "p2"}, 1)
		// Duplicate a card in hand
		s.Hands["p1"] = append(s.Hands["p1"], Card{Colour: ColourRed, Value: "1"})
		raw, _ := s.ExportSnapshot()
		_, err := RestoreFromSnapshot(raw)
		if err == nil || !strings.Contains(err.Error(), "108") {
			t.Fatalf("expected 108 card invariant violation, got %v", err)
		}
	})

	t.Run("invalid direction", func(t *testing.T) {
		s, _ := NewGame("m1", []string{"p1", "p2"}, 1)
		s.Direction = 0
		raw, _ := s.ExportSnapshot()
		_, err := RestoreFromSnapshot(raw)
		if err == nil {
			t.Fatal("expected validation failure for invalid direction")
		}
	})

	t.Run("finished game with cards in winner hand", func(t *testing.T) {
		s, _ := NewGame("m1", []string{"p1", "p2"}, 1)
		s.Status = StatusFinished
		s.Winner = "p1"
		raw, _ := s.ExportSnapshot()
		_, err := RestoreFromSnapshot(raw)
		if err == nil {
			t.Fatal("expected validation failure for non-empty winner hand")
		}
	})
}
