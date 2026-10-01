package uno

import (
	"fmt"
	"math/rand"
)

// reshuffleSalt spreads successive reshuffle seeds apart so that consecutive
// reshuffles of the same match do not produce correlated orderings.
const reshuffleSalt int64 = 0x2545F4914F6CDD1D

// shuffleDeterministic shuffles cards in place using only the supplied seed.
// Given the same seed and the same input order it always produces the same
// output, which is what lets a promoted backup pod reproduce the primary's
// card sequence exactly.
func shuffleDeterministic(cards []Card, seed int64) {
	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(cards), func(i, j int) {
		cards[i], cards[j] = cards[j], cards[i]
	})
}

// NewGame deals a fresh match.
//
// The deck is shuffled exactly once, here, from the supplied seed; the result
// is stored in the returned State. No randomness is consulted again for the
// rest of the match, so the state snapshot is a complete description of every
// future card draw.
//
// The starting discard is forced to be a number card: any action or wild card
// encountered while turning up the first card is moved to the back of the
// deck. This is a deliberate simplification -- it removes the ambiguity of
// applying an action card's effect before any player has had a turn, without
// affecting the distribution of the remaining deck.
func NewGame(matchID string, players []string, seed int64) (*State, error) {
	if len(players) < MinPlayers {
		return nil, ErrTooFewPlayers
	}
	if len(players) > MaxPlayers {
		return nil, fmt.Errorf("%w: %d (max %d)", ErrTooManyPlayers, len(players), MaxPlayers)
	}

	seen := make(map[string]struct{}, len(players))
	for _, p := range players {
		if p == "" {
			return nil, ErrEmptyPlayerID
		}
		if _, dup := seen[p]; dup {
			return nil, fmt.Errorf("%w: %q", ErrDuplicatePlayer, p)
		}
		seen[p] = struct{}{}
	}

	deck := NewDeck()
	shuffleDeterministic(deck, seed)

	s := &State{
		MatchID:            matchID,
		Sequence:           0,
		Status:             StatusInProgress,
		Players:            append([]string(nil), players...),
		Hands:              make(map[string][]Card, len(players)),
		Deck:               deck,
		DiscardPile:        make([]Card, 0, 16),
		CurrentPlayerIndex: 0,
		Direction:          DirectionClockwise,
		Seed:               seed,
	}

	// Deal one card at a time, round by round, as at a real table.
	for _, p := range players {
		s.Hands[p] = make([]Card, 0, HandSize)
	}
	for i := 0; i < HandSize; i++ {
		for _, p := range players {
			card := s.Deck[0]
			s.Deck = s.Deck[1:]
			s.Hands[p] = append(s.Hands[p], card)
		}
	}

	// Turn up the starting card, pushing non-number cards to the back.
	for {
		if len(s.Deck) == 0 {
			// Unreachable with a standard deck and <= MaxPlayers players.
			return nil, ErrDeckExhausted
		}
		card := s.Deck[0]
		s.Deck = s.Deck[1:]
		if card.IsAction() {
			s.Deck = append(s.Deck, card)
			continue
		}
		s.DiscardPile = append(s.DiscardPile, card)
		s.CurrentColour = card.Colour
		break
	}

	return s, nil
}

// matchesTable reports whether a non-wild card may be played on the current
// top card, by colour or by face value.
func matchesTable(s *State, card Card) bool {
	if card.Colour == s.CurrentColour {
		return true
	}
	top, ok := s.TopCard()
	if !ok {
		return false
	}
	// A value match is only meaningful between two non-wild cards: no
	// coloured card shares a face value with a wild.
	return !top.IsWild() && top.Value == card.Value
}

// holdsCurrentColour reports whether the player holds any card of the colour
// currently in force. A Wild Draw Four is only legal when they do not.
func holdsCurrentColour(s *State, playerID string) bool {
	for _, c := range s.Hands[playerID] {
		if c.Colour == s.CurrentColour {
			return true
		}
	}
	return false
}

// IsPlayable reports whether playerID could legally play card right now,
// including the Wild Draw Four restriction.
func IsPlayable(s *State, playerID string, card Card) bool {
	if card.Value == ValueWild {
		return true
	}
	if card.Value == ValueWildDraw {
		return !holdsCurrentColour(s, playerID)
	}
	return matchesTable(s, card)
}

// HasPlayableCard reports whether the player holds at least one legal move.
func HasPlayableCard(s *State, playerID string) bool {
	for _, c := range s.Hands[playerID] {
		if IsPlayable(s, playerID, c) {
			return true
		}
	}
	return false
}

// reshuffleDiscard recycles the discard pile (all but the top card) back into
// the draw pile. The ordering is derived from the stored seed and the
// reshuffle count, so it is reproducible from the snapshot alone.
func reshuffleDiscard(s *State) {
	if len(s.DiscardPile) <= 1 {
		return
	}
	top := s.DiscardPile[len(s.DiscardPile)-1]
	recycled := make([]Card, len(s.DiscardPile)-1)
	copy(recycled, s.DiscardPile[:len(s.DiscardPile)-1])

	// A wild card loses its declared colour when it returns to the deck.
	for i := range recycled {
		if recycled[i].IsWild() {
			recycled[i].Colour = ColourWild
		}
	}

	s.ReshuffleCount++
	shuffleDeterministic(recycled, s.Seed^(int64(s.ReshuffleCount)*reshuffleSalt))
	s.Deck = append(s.Deck, recycled...)
	s.DiscardPile = []Card{top}
}

// drawCards moves up to n cards from the draw pile into a player's hand,
// reshuffling the discard pile if the draw pile runs dry. It returns the
// number actually drawn, which is less than n only if the match has run out
// of cards entirely.
func drawCards(s *State, playerID string, n int) int {
	drawn := 0
	for drawn < n {
		if len(s.Deck) == 0 {
			reshuffleDiscard(s)
			if len(s.Deck) == 0 {
				break
			}
		}
		card := s.Deck[0]
		s.Deck = s.Deck[1:]
		s.Hands[playerID] = append(s.Hands[playerID], card)
		drawn++
	}
	return drawn
}

// advance moves the turn pointer by steps seats in the current direction.
func advance(s *State, steps int) {
	n := len(s.Players)
	if n == 0 {
		return
	}
	idx := (s.CurrentPlayerIndex + s.Direction*steps) % n
	if idx < 0 {
		idx += n
	}
	s.CurrentPlayerIndex = idx
	s.DrewThisTurn = false
}

// checkTurn validates that playerID is a known participant whose turn it is
// and that the match is still running.
func checkTurn(s *State, playerID string) error {
	if s.Status == StatusFinished {
		return ErrGameOver
	}
	if s.playerIndex(playerID) < 0 {
		return fmt.Errorf("%w: %q", ErrUnknownPlayer, playerID)
	}
	if s.CurrentPlayer() != playerID {
		return fmt.Errorf("%w: it is %q's turn", ErrNotYourTurn, s.CurrentPlayer())
	}
	return nil
}

// Play plays the card at cardIndex from playerID's hand.
//
// declaredColour must be one of the four real colours when the card is a wild,
// and empty otherwise. On success the returned State is the new state; on
// error the input state is left untouched.
func Play(s *State, playerID string, cardIndex int, declaredColour Colour) (*State, error) {
	if err := checkTurn(s, playerID); err != nil {
		return nil, err
	}

	hand := s.Hands[playerID]
	if cardIndex < 0 || cardIndex >= len(hand) {
		return nil, fmt.Errorf("%w: %d (hand size %d)", ErrCardIndex, cardIndex, len(hand))
	}
	card := hand[cardIndex]

	if card.IsWild() {
		if !IsPlayableColour(declaredColour) {
			return nil, fmt.Errorf("%w: got %q", ErrColourRequired, declaredColour)
		}
	} else if declaredColour != "" {
		return nil, fmt.Errorf("%w: %s is not wild", ErrColourNotAllowed, card)
	}

	switch {
	case card.Value == ValueWild:
		// Always legal.
	case card.Value == ValueWildDraw:
		if holdsCurrentColour(s, playerID) {
			return nil, ErrWildDrawFour
		}
	default:
		if !matchesTable(s, card) {
			top, _ := s.TopCard()
			return nil, fmt.Errorf("%w: played %s on %s (colour in force: %s)",
				ErrIllegalPlay, card, top, s.CurrentColour)
		}
	}

	next := s.Clone()

	// Remove the card from the hand, preserving the order of the rest.
	nextHand := next.Hands[playerID]
	nextHand = append(nextHand[:cardIndex], nextHand[cardIndex+1:]...)
	next.Hands[playerID] = nextHand

	// The wild keeps its colourless identity on the pile; the colour in force
	// is tracked separately in CurrentColour.
	next.DiscardPile = append(next.DiscardPile, card)
	if card.IsWild() {
		next.CurrentColour = declaredColour
	} else {
		next.CurrentColour = card.Colour
	}

	// Apply the card's effect. With exactly two players a Reverse has the
	// same practical effect as a Skip, which is the official rule.
	steps := 1
	twoPlayer := len(next.Players) == 2

	switch card.Value {
	case ValueSkip:
		steps = 2
	case ValueReverse:
		next.Direction = -next.Direction
		if twoPlayer {
			steps = 2
		}
	case ValueDrawTwo:
		drawCards(next, playerAt(next, 1), 2)
		steps = 2
	case ValueWildDraw:
		drawCards(next, playerAt(next, 1), 4)
		steps = 2
	}

	next.Sequence++

	if len(next.Hands[playerID]) == 0 {
		next.Status = StatusFinished
		next.Winner = playerID
		next.DrewThisTurn = false
		return next, nil
	}

	advance(next, steps)
	return next, nil
}

// playerAt returns the id of the player offset seats away from the current
// player in the current direction.
func playerAt(s *State, offset int) string {
	n := len(s.Players)
	idx := (s.CurrentPlayerIndex + s.Direction*offset) % n
	if idx < 0 {
		idx += n
	}
	return s.Players[idx]
}

// Draw takes a single card for playerID.
//
// If the drawn card is playable the turn stays with them, so they may play it
// or Pass. If it is not playable the turn advances immediately, since they
// have no further options.
func Draw(s *State, playerID string) (*State, error) {
	if err := checkTurn(s, playerID); err != nil {
		return nil, err
	}
	if s.DrewThisTurn {
		return nil, ErrAlreadyDrew
	}

	next := s.Clone()
	if drawCards(next, playerID, 1) == 0 {
		return nil, ErrDeckExhausted
	}
	next.DrewThisTurn = true
	next.Sequence++

	hand := next.Hands[playerID]
	drawn := hand[len(hand)-1]
	if !IsPlayable(next, playerID, drawn) {
		advance(next, 1)
	}
	return next, nil
}

// Pass ends the turn after a draw that left the player able to act.
// It is illegal before drawing: a player must always draw before giving up
// their turn.
func Pass(s *State, playerID string) (*State, error) {
	if err := checkTurn(s, playerID); err != nil {
		return nil, err
	}
	if !s.DrewThisTurn {
		return nil, ErrMustDrawFirst
	}

	next := s.Clone()
	next.Sequence++
	advance(next, 1)
	return next, nil
}
