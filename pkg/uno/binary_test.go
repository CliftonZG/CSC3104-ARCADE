package uno

import (
	"bytes"
	"testing"
)

func TestBinarySnapshotRoundTrip(t *testing.T) {
	s, err := NewGame("binary-match", []string{"alice", "bob", "carol"}, 999)
	if err != nil {
		t.Fatalf("NewGame failed: %v", err)
	}

	// Play a few turns
	for i := 0; i < 6; i++ {
		cur := s.CurrentPlayer()
		hand := s.Hands[cur]
		played := false
		for idx, c := range hand {
			if IsPlayable(s, cur, c) {
				var col Colour
				if c.IsWild() {
					col = ColourBlue
				}
				if next, err := Play(s, cur, idx, col); err == nil {
					s = next
					played = true
					break
				}
			}
		}
		if !played {
			s, _ = Draw(s, cur)
		}
	}

	bin, err := s.ExportBinary()
	if err != nil {
		t.Fatalf("ExportBinary failed: %v", err)
	}

	t.Logf("Binary snapshot size: %d bytes (vs JSON ~4000 bytes)", len(bin))

	restored, err := RestoreFromBinary(bin)
	if err != nil {
		t.Fatalf("RestoreFromBinary failed: %v", err)
	}

	// Assert bit-for-bit field equality
	if restored.MatchID != s.MatchID {
		t.Errorf("MatchID mismatch: got %s, want %s", restored.MatchID, s.MatchID)
	}
	if restored.Sequence != s.Sequence {
		t.Errorf("Sequence mismatch: got %d, want %d", restored.Sequence, s.Sequence)
	}
	if restored.CurrentPlayerIndex != s.CurrentPlayerIndex {
		t.Errorf("CurrentPlayerIndex mismatch: got %d, want %d", restored.CurrentPlayerIndex, s.CurrentPlayerIndex)
	}
	if restored.CurrentColour != s.CurrentColour {
		t.Errorf("CurrentColour mismatch: got %s, want %s", restored.CurrentColour, s.CurrentColour)
	}

	// Verify Checksums match
	chk1, _ := s.Checksum()
	chk2, _ := restored.Checksum()
	if chk1 != chk2 {
		t.Fatalf("Checksum mismatch after binary restore: %s != %s", chk1, chk2)
	}
}

func TestBinarySnapshotDeterminism(t *testing.T) {
	g1, _ := NewGame("det-match", []string{"p1", "p2"}, 12345)
	g2, _ := NewGame("det-match", []string{"p1", "p2"}, 12345)

	b1, err := g1.ExportBinary()
	if err != nil {
		t.Fatalf("g1 ExportBinary failed: %v", err)
	}
	b2, err := g2.ExportBinary()
	if err != nil {
		t.Fatalf("g2 ExportBinary failed: %v", err)
	}

	if !bytes.Equal(b1, b2) {
		t.Fatal("Binary snapshots from identical games are NOT bit-for-bit equal")
	}
}
