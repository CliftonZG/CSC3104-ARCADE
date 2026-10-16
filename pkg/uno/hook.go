package uno

import (
	"fmt"
	"sync"
)

// MoveHook is an observer callback executed synchronously after every valid move.
// It receives the state immediately before the action, the state immediately
// after the action, and the action summary.
type MoveHook func(prevState *State, nextState *State, summary ActionSummary) error

// hookRegistry manages a thread-safe list of MoveHook callbacks.
type hookRegistry struct {
	mu    sync.RWMutex
	hooks []MoveHook
}

// newHookRegistry initializes an empty hook registry.
func newHookRegistry() *hookRegistry {
	return &hookRegistry{
		hooks: make([]MoveHook, 0, 4),
	}
}

// Register appends a new MoveHook callback to the registry.
func (r *hookRegistry) Register(hook MoveHook) {
	if hook == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.hooks = append(r.hooks, hook)
}

// Dispatch executes all registered hooks in order.
// If any hook fails, dispatch stops and returns ErrHookFailed.
func (r *hookRegistry) Dispatch(prev, next *State, summary ActionSummary) error {
	r.mu.RLock()
	if len(r.hooks) == 0 {
		r.mu.RUnlock()
		return nil
	}
	active := make([]MoveHook, len(r.hooks))
	copy(active, r.hooks)
	r.mu.RUnlock()

	for _, h := range active {
		if err := h(prev, next, summary); err != nil {
			return fmt.Errorf("%w: %w", ErrHookFailed, err)
		}
	}
	return nil
}
