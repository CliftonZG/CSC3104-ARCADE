package uno

import "errors"

// Status is the lifecycle state of a match.
type Status string

const (
	StatusInProgress Status = "in_progress"
	StatusFinished   Status = "finished"
)

// Direction of play. It flips sign when a Reverse card is played.
const (
	DirectionClockwise        = 1
	DirectionCounterClockwise = -1
)

// HandSize is the number of cards dealt to each player at the start.
const HandSize = 7

// MinPlayers and MaxPlayers bound the number of participants in a match.
const (
	MinPlayers = 2
	MaxPlayers = 10
)

// Engine errors. Callers should compare with errors.Is.
var (
	ErrTooFewPlayers    = errors.New("uno: need at least 2 players")
	ErrTooManyPlayers   = errors.New("uno: too many players")
	ErrDuplicatePlayer  = errors.New("uno: duplicate player id")
	ErrEmptyPlayerID    = errors.New("uno: empty player id")
	ErrGameOver         = errors.New("uno: game already finished")
	ErrNotYourTurn      = errors.New("uno: not your turn")
	ErrUnknownPlayer    = errors.New("uno: unknown player")
	ErrCardIndex        = errors.New("uno: card index out of range")
	ErrIllegalPlay      = errors.New("uno: card does not match the current colour or value")
	ErrWildDrawFour     = errors.New("uno: wild draw four is only playable with no card of the current colour")
	ErrColourRequired   = errors.New("uno: a wild card requires a declared colour")
	ErrColourNotAllowed = errors.New("uno: only a wild card may declare a colour")
	ErrAlreadyDrew      = errors.New("uno: already drew this turn")
	ErrMustDrawFirst    = errors.New("uno: cannot pass before drawing")
	ErrDeckExhausted    = errors.New("uno: no cards left to draw")
)

// State is the complete, serialisable state of one match.
//
// Every field is exported and JSON-tagged: the struct round-trips through
// Redis without loss, which is what allows a backup pod to resume a match
// from a snapshot. The JSON keys match the schema shared with the proxy.
type State struct {
	MatchID string `json:"match_id"`

	// Sequence increments on every state-changing action. The control plane
	// uses it to confirm a backup holds the latest snapshot before promotion.
	Sequence int64 `json:"sequence"`

	Status Status `json:"status"`

	// Players is the seating order. Index positions are stable for the
	// lifetime of the match.
	Players []string `json:"players"`

	// Hands maps player id to that player's cards.
	Hands map[string][]Card `json:"hands"`

	// Deck is the remaining draw pile; index 0 is the next card drawn.
	Deck []Card `json:"deck"`

	// DiscardPile grows as cards are played; the last element is the top card.
	DiscardPile []Card `json:"discard_pile"`

	// CurrentColour is the colour in force. It differs from the top card's
	// own colour after a wild is played, because the wild itself is colourless.
	CurrentColour Colour `json:"current_color"`

	CurrentPlayerIndex int `json:"current_player_index"`

	// Direction is +1 or -1; a Reverse card multiplies it by -1.
	Direction int `json:"direction"`

	// DrewThisTurn records that the current player has already drawn, which
	// permits them to pass and forbids a second draw.
	DrewThisTurn bool `json:"drew_this_turn"`

	Winner string `json:"winner"`

	// Seed is the shuffle seed chosen once at match creation and then stored.
	// Reshuffles derive from it, so no live randomness is ever needed again.
	Seed int64 `json:"seed"`

	// ReshuffleCount is the number of times the discard pile has been
	// recycled into the deck. Combined with Seed it makes each reshuffle
	// reproducible by any pod holding the snapshot.
	ReshuffleCount int `json:"reshuffle_count"`
}

// TopCard returns the card currently on top of the discard pile.
// The second return value is false only if the pile is empty, which cannot
// happen in a state produced by NewGame.
func (s *State) TopCard() (Card, bool) {
	if len(s.DiscardPile) == 0 {
		return Card{}, false
	}
	return s.DiscardPile[len(s.DiscardPile)-1], true
}

// CurrentPlayer returns the id of the player whose turn it is.
func (s *State) CurrentPlayer() string {
	if len(s.Players) == 0 {
		return ""
	}
	return s.Players[s.CurrentPlayerIndex]
}

// Hand returns a copy of a player's hand, so callers cannot mutate state.
func (s *State) Hand(playerID string) ([]Card, bool) {
	hand, ok := s.Hands[playerID]
	if !ok {
		return nil, false
	}
	out := make([]Card, len(hand))
	copy(out, hand)
	return out, true
}

// Clone returns a deep copy of the state. The engine mutates a clone and
// only swaps it in once an action has fully succeeded, so a rejected action
// never leaves the caller's state half-modified.
func (s *State) Clone() *State {
	if s == nil {
		return nil
	}
	out := *s

	out.Players = make([]string, len(s.Players))
	copy(out.Players, s.Players)

	out.Hands = make(map[string][]Card, len(s.Hands))
	for id, hand := range s.Hands {
		cp := make([]Card, len(hand))
		copy(cp, hand)
		out.Hands[id] = cp
	}

	out.Deck = make([]Card, len(s.Deck))
	copy(out.Deck, s.Deck)

	out.DiscardPile = make([]Card, len(s.DiscardPile))
	copy(out.DiscardPile, s.DiscardPile)

	return &out
}

// playerIndex finds a player's seat, or -1 if they are not in the match.
func (s *State) playerIndex(playerID string) int {
	for i, p := range s.Players {
		if p == playerID {
			return i
		}
	}
	return -1
}
