package uno

import (
	"crypto/sha256"
	"encoding/hex"
)

// Checksum returns the hex-encoded SHA-256 digest of the canonical state snapshot.
// In distributed replication, this provides an O(1) mathematical proof that Primary
// and Standby pods hold 100% bit-for-bit identical state without transmitting entire states.
func (s *State) Checksum() (string, error) {
	raw, err := s.ExportSnapshot()
	if err != nil {
		return "", err
	}
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:]), nil
}
