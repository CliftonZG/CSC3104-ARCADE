package uno

import "fmt"

// ActionType identifies the action requested by a player.
type ActionType string

const (
	// ActionPlay requests playing a card from the player's hand.
	ActionPlay ActionType = "play"

	// ActionDraw requests drawing a card from the deck.
	ActionDraw ActionType = "draw"

	// ActionPass requests passing the turn after a legal draw.
	ActionPass ActionType = "pass"
)

// Action is the canonical command payload transmitted over IPC, RPC, or WebSockets.
type Action struct {
	Type           ActionType `json:"type"`
	PlayerID       string     `json:"player_id"`
	CardIndex      int        `json:"card_index,omitempty"`
	DeclaredColour Colour     `json:"declared_color,omitempty"`
}

// Validate checks basic sanity of the Action payload before engine execution.
func (a Action) Validate() error {
	if a.PlayerID == "" {
		return ErrEmptyPlayerID
	}
	switch a.Type {
	case ActionPlay:
		if a.CardIndex < 0 {
			return fmt.Errorf("%w: negative card index %d", ErrCardIndex, a.CardIndex)
		}
	case ActionDraw, ActionPass:
		// No additional fields required
	default:
		return fmt.Errorf("%w: %q", ErrUnknownAction, a.Type)
	}
	return nil
}
