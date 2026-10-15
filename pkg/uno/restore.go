package uno

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// RestoreFromSnapshot deserializes and rigorously validates a State from raw bytes.
// It returns a deeply isolated State with zero shared references to input buffers.
func RestoreFromSnapshot(raw []byte) (*State, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("%w: empty payload", ErrInvalidSnapshot)
	}

	var s State
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("%w: JSON unmarshal error: %v", ErrInvalidSnapshot, err)
	}

	if err := s.Validate(); err != nil {
		return nil, err
	}

	return &s, nil
}

// RestoreFromReader streams snapshot data directly from an io.Reader (e.g. network socket),
// decodes, and validates it.
func RestoreFromReader(r io.Reader) (*State, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: reader is nil", ErrInvalidSnapshot)
	}

	var s State
	dec := json.NewDecoder(r)
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("%w: JSON stream decode error: %v", ErrInvalidSnapshot, err)
	}

	if err := s.Validate(); err != nil {
		return nil, err
	}

	return &s, nil
}
