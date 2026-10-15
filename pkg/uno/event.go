package uno

// EventType classifies domain occurrences emitted when a move changes state.
type EventType string

const (
	EventCardPlayed        EventType = "card_played"
	EventCardDrawn         EventType = "card_drawn"
	EventTurnPassed        EventType = "turn_passed"
	EventTurnSkipped       EventType = "turn_skipped"
	EventDirectionReversed EventType = "direction_reversed"
	EventPenaltyDrawn      EventType = "penalty_drawn"
	EventColourDeclared    EventType = "colour_declared"
	EventDeckReshuffled    EventType = "deck_reshuffled"
	EventGameOver          EventType = "game_over"
)

// ActionSummary captures full semantic context of a committed turn for external listeners.
type ActionSummary struct {
	Sequence       int64       `json:"sequence"`
	Action         Action      `json:"action"`
	PlayedCard     *Card       `json:"played_card,omitempty"`
	DrawnCardCount int         `json:"drawn_card_count,omitempty"`
	Events         []EventType `json:"events,omitempty"`
	NextPlayer     string      `json:"next_player"`
	Winner         string      `json:"winner,omitempty"`
}

// buildActionSummary inspects the delta between prev and next State to compile events.
func buildActionSummary(prev, next *State, act Action) ActionSummary {
	summary := ActionSummary{
		Sequence:   next.Sequence,
		Action:     act,
		NextPlayer: next.CurrentPlayer(),
		Winner:     next.Winner,
	}

	events := make([]EventType, 0, 4)

	switch act.Type {
	case ActionPlay:
		events = append(events, EventCardPlayed)
		hand := prev.Hands[act.PlayerID]
		if act.CardIndex >= 0 && act.CardIndex < len(hand) {
			card := hand[act.CardIndex]
			summary.PlayedCard = &card

			if card.IsWild() {
				events = append(events, EventColourDeclared)
			}
			switch card.Value {
			case ValueSkip:
				events = append(events, EventTurnSkipped)
			case ValueReverse:
				if len(prev.Players) == 2 {
					events = append(events, EventTurnSkipped)
				} else {
					events = append(events, EventDirectionReversed)
				}
			case ValueDrawTwo:
				summary.DrawnCardCount = 2
				events = append(events, EventPenaltyDrawn, EventTurnSkipped)
			case ValueWildDraw:
				summary.DrawnCardCount = 4
				events = append(events, EventPenaltyDrawn, EventTurnSkipped)
			}
		}
	case ActionDraw:
		events = append(events, EventCardDrawn)
		summary.DrawnCardCount = 1
		if next.CurrentPlayerIndex != prev.CurrentPlayerIndex {
			events = append(events, EventTurnPassed)
		}
	case ActionPass:
		events = append(events, EventTurnPassed)
	}

	if next.ReshuffleCount > prev.ReshuffleCount {
		events = append(events, EventDeckReshuffled)
	}

	if next.Status == StatusFinished {
		events = append(events, EventGameOver)
	}

	summary.Events = events
	return summary
}
