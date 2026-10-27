package uno

import "sync"

// Game is a thread-safe, authoritative engine controller for an Uno match.
// It guards in-memory state transitions with read/write mutexes, dispatches
// move hooks upon successful moves, and manages hot snapshot synchronization.
type Game struct {
	mu    sync.RWMutex
	state *State
	hooks *hookRegistry
}

// NewGameEngine initializes a fresh, deterministic Uno game wrapped in the Game engine.
func NewGameEngine(matchID string, players []string, seed int64) (*Game, error) {
	st, err := NewGame(matchID, players, seed)
	if err != nil {
		return nil, err
	}
	return &Game{
		state: st,
		hooks: newHookRegistry(),
	}, nil
}

// NewGameEngineFromState creates a Game engine initialized with an existing State.
// The state is validated and deeply cloned to guarantee zero pointer leakage.
func NewGameEngineFromState(s *State) (*Game, error) {
	if s == nil {
		return nil, ErrNilState
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return &Game{
		state: s.Clone(),
		hooks: newHookRegistry(),
	}, nil
}

// RestoreGameFromSnapshot deserializes and validates a snapshot byte slice,
// returning an active Game ready to resume gameplay immediately on any pod.
func RestoreGameFromSnapshot(raw []byte) (*Game, error) {
	st, err := RestoreFromSnapshot(raw)
	if err != nil {
		return nil, err
	}
	return &Game{
		state: st,
		hooks: newHookRegistry(),
	}, nil
}

// RegisterHook adds a MoveHook listener to be called on every valid state transition.
func (g *Game) RegisterHook(hook MoveHook) {
	g.hooks.Register(hook)
}

// State returns a deep copy of the current state, preventing caller mutations
// from corrupting internal engine state.
func (g *Game) State() *State {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.Clone()
}

// ExportSnapshot serializes the internal state into canonical JSON bytes.
// Holds an RLock to guarantee snapshot consistency under concurrent operations.
func (g *Game) ExportSnapshot() ([]byte, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.ExportSnapshot()
}

// ExportBinary serializes the internal state into compact bit-packed binary bytes.
// Holds an RLock to guarantee snapshot consistency under concurrent operations.
func (g *Game) ExportBinary() ([]byte, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.ExportBinary()
}

// Checksum computes the SHA-256 fingerprint of the current state.
func (g *Game) Checksum() (string, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.Checksum()
}

// ApplySnapshot atomically replaces the engine's state with a verified snapshot.
func (g *Game) ApplySnapshot(data []byte) error {
	restored, err := RestoreFromSnapshot(data)
	if err != nil {
		return err
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	g.state = restored
	return nil
}

// MatchID returns the match identifier.
func (g *Game) MatchID() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.MatchID
}

// Sequence returns the current state sequence counter.
func (g *Game) Sequence() int64 {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.Sequence
}

// CurrentPlayer returns the player whose turn it currently is.
func (g *Game) CurrentPlayer() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.CurrentPlayer()
}

// IsFinished reports whether the game has concluded.
func (g *Game) IsFinished() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.Status == StatusFinished
}

// Winner returns the ID of the winner, or empty if the game is still running.
func (g *Game) Winner() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.Winner
}

// TopCard returns the current card on top of the discard pile.
func (g *Game) TopCard() (Card, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.state.TopCard()
}
