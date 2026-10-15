package uno

import "errors"

// Package-level error definitions for snapshot, engine, and action lifecycle.
var (
	// ErrInvalidSnapshot indicates that snapshot data could not be parsed or
	// failed the strict game state invariant checks.
	ErrInvalidSnapshot = errors.New("uno: invalid snapshot")

	// ErrNilState is returned when an operation is attempted on an uninitialized State.
	ErrNilState = errors.New("uno: state is nil")

	// ErrUnknownAction is returned when an Action has an unsupported ActionType.
	ErrUnknownAction = errors.New("uno: unknown action type")

	// ErrHookFailed wraps any error returned by a registered MoveHook listener.
	ErrHookFailed = errors.New("uno: move hook failed")
)
