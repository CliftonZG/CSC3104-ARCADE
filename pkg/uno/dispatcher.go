package uno

import "fmt"

// ApplyAction is the unified, thread-safe entry point for executing player actions.
// If the move is invalid, the engine state remains completely untouched.
// On success, the state pointer is atomically advanced and all registered move hooks
// are executed synchronously.
func (g *Game) ApplyAction(act Action) (*State, error) {
	if err := act.Validate(); err != nil {
		return nil, err
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	prev := g.state
	var next *State
	var err error

	switch act.Type {
	case ActionPlay:
		next, err = Play(prev, act.PlayerID, act.CardIndex, act.DeclaredColour)
	case ActionDraw:
		next, err = Draw(prev, act.PlayerID)
	case ActionPass:
		next, err = Pass(prev, act.PlayerID)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownAction, act.Type)
	}

	if err != nil {
		return nil, err
	}

	summary := buildActionSummary(prev, next, act)

	// Atomically swap in the new state
	g.state = next

	// Synchronously notify registered listeners (e.g. Redis snapshot persistence)
	if hookErr := g.hooks.Dispatch(prev, next, summary); hookErr != nil {
		return next.Clone(), hookErr
	}

	return next.Clone(), nil
}

// PlayCard executes a card play action for playerID.
func (g *Game) PlayCard(playerID string, cardIndex int, declaredColour Colour) (*State, error) {
	return g.ApplyAction(Action{
		Type:           ActionPlay,
		PlayerID:       playerID,
		CardIndex:      cardIndex,
		DeclaredColour: declaredColour,
	})
}

// DrawCard executes a draw card action for playerID.
func (g *Game) DrawCard(playerID string) (*State, error) {
	return g.ApplyAction(Action{
		Type:     ActionDraw,
		PlayerID: playerID,
	})
}

// PassTurn executes a pass turn action for playerID.
func (g *Game) PassTurn(playerID string) (*State, error) {
	return g.ApplyAction(Action{
		Type:     ActionPass,
		PlayerID: playerID,
	})
}
