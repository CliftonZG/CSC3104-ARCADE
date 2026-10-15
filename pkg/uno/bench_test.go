package uno

import (
	"testing"
)

func BenchmarkSnapshotExportJSON(b *testing.B) {
	s, err := NewGame("bench-match", []string{"p1", "p2", "p3", "p4"}, 100)
	if err != nil {
		b.Fatalf("NewGame failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := s.ExportSnapshot()
		if err != nil {
			b.Fatalf("ExportSnapshot failed: %v", err)
		}
	}
}

func BenchmarkSnapshotRestoreJSON(b *testing.B) {
	s, err := NewGame("bench-match", []string{"p1", "p2", "p3", "p4"}, 100)
	if err != nil {
		b.Fatalf("NewGame failed: %v", err)
	}
	raw, err := s.ExportSnapshot()
	if err != nil {
		b.Fatalf("ExportSnapshot failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := RestoreFromSnapshot(raw)
		if err != nil {
			b.Fatalf("RestoreFromSnapshot failed: %v", err)
		}
	}
}

func BenchmarkSnapshotExportBinary(b *testing.B) {
	s, err := NewGame("bench-match", []string{"p1", "p2", "p3", "p4"}, 100)
	if err != nil {
		b.Fatalf("NewGame failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := s.ExportBinary()
		if err != nil {
			b.Fatalf("ExportBinary failed: %v", err)
		}
	}
}

func BenchmarkSnapshotRestoreBinary(b *testing.B) {
	s, err := NewGame("bench-match", []string{"p1", "p2", "p3", "p4"}, 100)
	if err != nil {
		b.Fatalf("NewGame failed: %v", err)
	}
	raw, err := s.ExportBinary()
	if err != nil {
		b.Fatalf("ExportBinary failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := RestoreFromBinary(raw)
		if err != nil {
			b.Fatalf("RestoreFromBinary failed: %v", err)
		}
	}
}

func BenchmarkChecksum(b *testing.B) {
	s, err := NewGame("bench-match", []string{"p1", "p2", "p3", "p4"}, 100)
	if err != nil {
		b.Fatalf("NewGame failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := s.Checksum()
		if err != nil {
			b.Fatalf("Checksum failed: %v", err)
		}
	}
}

func BenchmarkEngineApplyAction(b *testing.B) {
	eng, err := NewGameEngine("bench-engine", []string{"p1", "p2"}, 200)
	if err != nil {
		b.Fatalf("NewGameEngine failed: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		cur := eng.CurrentPlayer()
		_, _ = eng.DrawCard(cur)
	}
}
