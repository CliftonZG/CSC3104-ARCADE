package uno

import "fmt"

// Validate rigorously checks that s satisfies every invariant of a legitimate Uno match.
// It guarantees that corrupt, tampered, or partial network payloads cannot crash
// the pod, desynchronize state, or allow card duplication.
func (s *State) Validate() error {
	if s == nil {
		return ErrNilState
	}
	if s.MatchID == "" {
		return fmt.Errorf("%w: match_id cannot be empty", ErrInvalidSnapshot)
	}

	seen := make(map[string]struct{}, len(s.Players))
	if err := s.validatePlayers(seen); err != nil {
		return err
	}
	if err := s.validateSeatingAndHands(); err != nil {
		return err
	}
	if err := s.validateStatePointers(); err != nil {
		return err
	}
	if err := s.validateStatusAndWinner(seen); err != nil {
		return err
	}
	return s.validateDeckInventoryAndComposition()
}

// validatePlayers checks player counts and enforces uniqueness without empty IDs.
func (s *State) validatePlayers(seen map[string]struct{}) error {
	if len(s.Players) < MinPlayers {
		return fmt.Errorf("%w: player count %d below min %d", ErrInvalidSnapshot, len(s.Players), MinPlayers)
	}
	if len(s.Players) > MaxPlayers {
		return fmt.Errorf("%w: player count %d exceeds max %d", ErrInvalidSnapshot, len(s.Players), MaxPlayers)
	}
	for _, p := range s.Players {
		if p == "" {
			return fmt.Errorf("%w: empty player id in player list", ErrInvalidSnapshot)
		}
		if _, dup := seen[p]; dup {
			return fmt.Errorf("%w: duplicate player id %q", ErrInvalidSnapshot, p)
		}
		seen[p] = struct{}{}
	}
	return nil
}

// validateSeatingAndHands ensures every seated player holds an allocated hand.
func (s *State) validateSeatingAndHands() error {
	if len(s.Hands) != len(s.Players) {
		return fmt.Errorf("%w: hands count %d does not match player count %d", ErrInvalidSnapshot, len(s.Hands), len(s.Players))
	}
	for _, p := range s.Players {
		if _, ok := s.Hands[p]; !ok {
			return fmt.Errorf("%w: missing hand entry for player %q", ErrInvalidSnapshot, p)
		}
	}
	return nil
}

// validateStatePointers checks indices, directions, and color in force.
func (s *State) validateStatePointers() error {
	if s.CurrentPlayerIndex < 0 || s.CurrentPlayerIndex >= len(s.Players) {
		return fmt.Errorf("%w: current_player_index %d out of bounds [0, %d)", ErrInvalidSnapshot, s.CurrentPlayerIndex, len(s.Players))
	}
	if s.Direction != DirectionClockwise && s.Direction != DirectionCounterClockwise {
		return fmt.Errorf("%w: invalid direction %d (must be 1 or -1)", ErrInvalidSnapshot, s.Direction)
	}
	if s.Status != StatusInProgress && s.Status != StatusFinished {
		return fmt.Errorf("%w: invalid status %q", ErrInvalidSnapshot, s.Status)
	}
	if len(s.DiscardPile) == 0 {
		return fmt.Errorf("%w: discard pile cannot be empty", ErrInvalidSnapshot)
	}
	if !IsPlayableColour(s.CurrentColour) {
		return fmt.Errorf("%w: invalid current_color %q", ErrInvalidSnapshot, s.CurrentColour)
	}
	return nil
}

// validateStatusAndWinner validates termination semantics and winning conditions.
func (s *State) validateStatusAndWinner(seen map[string]struct{}) error {
	if s.Status == StatusFinished {
		if s.Winner == "" {
			return fmt.Errorf("%w: finished game must have a winner", ErrInvalidSnapshot)
		}
		if _, ok := seen[s.Winner]; !ok {
			return fmt.Errorf("%w: winner %q is not in players list", ErrInvalidSnapshot, s.Winner)
		}
		if len(s.Hands[s.Winner]) != 0 {
			return fmt.Errorf("%w: winner %q hand must be empty (has %d cards)", ErrInvalidSnapshot, s.Winner, len(s.Hands[s.Winner]))
		}
		return nil
	}
	if s.Winner != "" {
		return fmt.Errorf("%w: in-progress game must not have a winner (got %q)", ErrInvalidSnapshot, s.Winner)
	}
	return nil
}

// validateDeckInventoryAndComposition verifies 108 cards conservation law and canonical Uno composition.
func (s *State) validateDeckInventoryAndComposition() error {
	totalCards := len(s.Deck) + len(s.DiscardPile)
	for _, hand := range s.Hands {
		totalCards += len(hand)
	}
	if totalCards != 108 {
		return fmt.Errorf("%w: total card count invariant violated: got %d, want 108", ErrInvalidSnapshot, totalCards)
	}

	cardCounts := make(map[Card]int, 108)
	if err := s.countCardsInLocations(cardCounts); err != nil {
		return err
	}
	return verifyDeckCompositionCounts(cardCounts)
}

// countCardsInLocations populates cardCounts and validates individual card properties.
func (s *State) countCardsInLocations(cardCounts map[Card]int) error {
	countCard := func(c Card, location string) error {
		if c.IsWild() {
			if c.Value != ValueWild && c.Value != ValueWildDraw {
				return fmt.Errorf("%w: invalid wild value %q at %s", ErrInvalidSnapshot, c.Value, location)
			}
			cardCounts[Card{Colour: ColourWild, Value: c.Value}]++
			return nil
		}
		if !IsPlayableColour(c.Colour) {
			return fmt.Errorf("%w: invalid card colour %q at %s", ErrInvalidSnapshot, c.Colour, location)
		}
		cardCounts[c]++
		return nil
	}

	for _, c := range s.Deck {
		if err := countCard(c, "deck"); err != nil {
			return err
		}
	}
	for _, c := range s.DiscardPile {
		if err := countCard(c, "discard pile"); err != nil {
			return err
		}
	}
	for p, hand := range s.Hands {
		for _, c := range hand {
			if err := countCard(c, "hand of "+p); err != nil {
				return err
			}
		}
	}
	return nil
}

// verifyDeckCompositionCounts asserts exact Uno canonical card distributions.
func verifyDeckCompositionCounts(counts map[Card]int) error {
	for _, col := range Colours() {
		if counts[Card{Colour: col, Value: "0"}] != 1 {
			return fmt.Errorf("%w: composition error: %s 0 count = %d, want 1", ErrInvalidSnapshot, col, counts[Card{Colour: col, Value: "0"}])
		}
		for _, val := range numberValues[1:] {
			if counts[Card{Colour: col, Value: val}] != 2 {
				return fmt.Errorf("%w: composition error: %s %s count = %d, want 2", ErrInvalidSnapshot, col, val, counts[Card{Colour: col, Value: val}])
			}
		}
		for _, val := range actionValues {
			if counts[Card{Colour: col, Value: val}] != 2 {
				return fmt.Errorf("%w: composition error: %s %s count = %d, want 2", ErrInvalidSnapshot, col, val, counts[Card{Colour: col, Value: val}])
			}
		}
	}
	if counts[Card{Colour: ColourWild, Value: ValueWild}] != 4 {
		return fmt.Errorf("%w: composition error: wild count = %d, want 4", ErrInvalidSnapshot, counts[Card{Colour: ColourWild, Value: ValueWild}])
	}
	if counts[Card{Colour: ColourWild, Value: ValueWildDraw}] != 4 {
		return fmt.Errorf("%w: composition error: wild4 count = %d, want 4", ErrInvalidSnapshot, counts[Card{Colour: ColourWild, Value: ValueWildDraw}])
	}
	return nil
}
