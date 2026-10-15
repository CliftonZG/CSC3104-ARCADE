package uno

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// ExportSnapshot serializes the State into a compact, canonical JSON byte slice.
// It leverages buffer pooling to minimize GC allocations and memory fragmentation.
func (s *State) ExportSnapshot() ([]byte, error) {
	if s == nil {
		return nil, ErrNilState
	}

	buf := getBuffer()
	defer putBuffer(buf)

	enc := json.NewEncoder(buf)
	if err := enc.Encode(s); err != nil {
		return nil, fmt.Errorf("%w: failed to encode state: %v", ErrInvalidSnapshot, err)
	}

	// Trim trailing newline added by json.Encoder for canonical wire format
	b := bytes.TrimRight(buf.Bytes(), "\n")
	out := make([]byte, len(b))
	copy(out, b)
	return out, nil
}

// ExportSnapshotToWriter writes the snapshot directly to an io.Writer (e.g. socket,
// file, or Redis pipeline), cutting intermediate buffer copies to reduce syscall latency.
func (s *State) ExportSnapshotToWriter(w io.Writer) error {
	if s == nil {
		return ErrNilState
	}
	if w == nil {
		return fmt.Errorf("%w: writer is nil", ErrInvalidSnapshot)
	}
	enc := json.NewEncoder(w)
	if err := enc.Encode(s); err != nil {
		return fmt.Errorf("%w: failed to stream state: %v", ErrInvalidSnapshot, err)
	}
	return nil
}

// ApplySnapshot deserializes raw snapshot bytes, validates state invariants,
// and atomically updates s in-place. If validation fails, s is left unmodified.
func (s *State) ApplySnapshot(data []byte) error {
	if s == nil {
		return ErrNilState
	}
	restored, err := RestoreFromSnapshot(data)
	if err != nil {
		return err
	}
	*s = *restored
	return nil
}
