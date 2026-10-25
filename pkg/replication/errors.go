package replication

import (
	"errors"
	"fmt"
)

var (
	// ErrFenced is returned when a write or epoch claim carries an epoch that is
	// no longer current: another pod has been promoted and this pod's write
	// access is revoked. Match with errors.Is; use errors.As with *FencedError
	// for the epoch details.
	ErrFenced = errors.New("replication: fenced by newer epoch")

	// ErrStaleSequence is returned when a write's sequence is not newer than the
	// snapshot already stored for the current epoch.
	ErrStaleSequence = errors.New("replication: stale snapshot sequence")

	// ErrNoSnapshot is returned when a match has no snapshot in Redis yet.
	ErrNoSnapshot = errors.New("replication: no snapshot for match")

	// ErrUnknownFormat is returned for a snapshot format other than JSON or binary.
	ErrUnknownFormat = errors.New("replication: unknown snapshot format")
)

// FencedError carries the epoch the caller held and the epoch Redis holds.
type FencedError struct {
	MatchID string
	Held    int64
	Current int64
}

func (e *FencedError) Error() string {
	return fmt.Sprintf("replication: match %q fenced: held epoch %d, current epoch %d", e.MatchID, e.Held, e.Current)
}

// Is reports a FencedError as ErrFenced.
func (e *FencedError) Is(target error) bool {
	return target == ErrFenced
}
