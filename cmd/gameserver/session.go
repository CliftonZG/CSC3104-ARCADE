package main

import (
	"fmt"
	"sync"

	"github.com/CSC3104-ARCADE/pkg/uno"
)

// EngineSession defines the decoupled interface between the backend transport layer
// (WebSockets, HTTP handlers, failure detector) and the core Uno game state machine.
type EngineSession interface {
	ApplyAction(act uno.Action) (*uno.State, error)
	State() *uno.State
	Sequence() int64
	MatchID() string
	ExportSnapshot() ([]byte, error)
	ExportBinary() ([]byte, error)
	ActiveGame() *uno.Game
	SwapGame(newGame *uno.Game)
	RegisterHook(hook uno.MoveHook)
	SetReplicatorHook(hook uno.MoveHook)
}

// GameSession provides a thread-safe, decoupled implementation of EngineSession.
// It manages the active Uno Game Engine, guarantees persistent hook preservation
// across active-standby failovers, and decouples the transport layer from raw pointer mutations.
type GameSession struct {
	mu      sync.RWMutex
	game    *uno.Game
	matchID string
	hooks   []uno.MoveHook
	repHook uno.MoveHook
}

// NewGameSession initializes a session with an active game instance.
func NewGameSession(initialGame *uno.Game) *GameSession {
	var matchID string
	if initialGame != nil {
		matchID = initialGame.MatchID()
	}
	return &GameSession{
		game:    initialGame,
		matchID: matchID,
	}
}

// RegisterHook registers a persistent MoveHook. All hooks registered on the session
// are guaranteed to survive engine reloads and primary takeovers.
func (s *GameSession) RegisterHook(hook uno.MoveHook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hooks = append(s.hooks, hook)
	if s.game != nil {
		s.game.RegisterHook(hook)
	}
}

// SetReplicatorHook sets the single active replication MoveHook, replacing any
// prior epoch's replicator hook so stale hooks are never re-attached on takeover.
func (s *GameSession) SetReplicatorHook(hook uno.MoveHook) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.repHook = hook
	if s.game != nil && hook != nil {
		s.game.RegisterHook(hook)
	}
}

// SwapGame safely hot-swaps the underlying engine (e.g. after Standby promotion and snapshot restore)
// and re-attaches all registered persistent hooks and the active replicator hook atomically.
func (s *GameSession) SwapGame(newGame *uno.Game) {
	if newGame == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.game = newGame
	s.matchID = newGame.MatchID()
	for _, h := range s.hooks {
		s.game.RegisterHook(h)
	}
	if s.repHook != nil {
		s.game.RegisterHook(s.repHook)
	}
}

// ApplyAction executes a move on the active game state in a thread-safe manner.
func (s *GameSession) ApplyAction(act uno.Action) (*uno.State, error) {
	s.mu.RLock()
	g := s.game
	s.mu.RUnlock()

	if g == nil {
		return nil, fmt.Errorf("session: game engine not initialized")
	}
	return g.ApplyAction(act)
}

// State returns a deep copy of the current state.
func (s *GameSession) State() *uno.State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.game == nil {
		return nil
	}
	return s.game.State()
}

// Sequence returns the monotonically increasing sequence number of the current state.
func (s *GameSession) Sequence() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.game == nil {
		return 0
	}
	return s.game.Sequence()
}

// MatchID returns the unique match identifier.
func (s *GameSession) MatchID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.matchID
}

// ExportSnapshot exports the current state as a canonical JSON snapshot.
func (s *GameSession) ExportSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.game == nil {
		return nil, fmt.Errorf("session: game engine not initialized")
	}
	return s.game.ExportSnapshot()
}

// ExportBinary exports the current state as an ultra-compact bit-packed binary payload.
func (s *GameSession) ExportBinary() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.game == nil {
		return nil, fmt.Errorf("session: game engine not initialized")
	}
	return s.game.ExportBinary()
}

// ActiveGame returns the underlying *uno.Game pointer.
func (s *GameSession) ActiveGame() *uno.Game {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.game
}
