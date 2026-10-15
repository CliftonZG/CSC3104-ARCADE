package uno

import "testing"

func FuzzRestoreFromSnapshot(f *testing.F) {
	// Seed with valid snapshot
	s, _ := NewGame("fuzz-seed", []string{"p1", "p2"}, 123)
	snap, _ := s.ExportSnapshot()
	f.Add(snap)
	f.Add([]byte(`{}`))
	f.Add([]byte(`{"match_id":"invalid"`))
	f.Add([]byte(`{"deck": null}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		// RestoreFromSnapshot must NEVER panic, regardless of input corruption
		_, _ = RestoreFromSnapshot(data)
	})
}

func FuzzRestoreFromBinary(f *testing.F) {
	// Seed with valid binary snapshot
	s, _ := NewGame("fuzz-seed", []string{"p1", "p2"}, 123)
	bin, _ := s.ExportBinary()
	f.Add(bin)
	f.Add([]byte{})
	f.Add([]byte{0x55, 0x4E, 0x4F, 0x31}) // partial magic

	f.Fuzz(func(t *testing.T, data []byte) {
		// RestoreFromBinary must NEVER panic
		_, _ = RestoreFromBinary(data)
	})
}
