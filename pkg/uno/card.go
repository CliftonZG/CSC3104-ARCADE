// Package uno implements a deterministic, serialisable Uno game engine.
//
// Determinism is the central design constraint: the engine never calls a
// random source after the initial deck construction. The shuffled deck is
// stored in the game state itself, so a backup pod that reloads a snapshot
// replays exactly the same card sequence the crashed primary would have.
package uno

import "fmt"

// Colour is the colour of a card. Wild cards carry ColourWild until played,
// at which point the player declares one of the four real colours.
type Colour string

const (
	ColourRed    Colour = "red"
	ColourYellow Colour = "yellow"
	ColourGreen  Colour = "green"
	ColourBlue   Colour = "blue"
	ColourWild   Colour = "wild"
)

// Colours lists the four playable colours (excluding ColourWild).
// Declared as a function rather than a package-level slice so callers
// cannot mutate the shared backing array.
func Colours() []Colour {
	return []Colour{ColourRed, ColourYellow, ColourGreen, ColourBlue}
}

// IsPlayableColour reports whether c is one of the four real colours.
// ColourWild is not a playable declaration.
func IsPlayableColour(c Colour) bool {
	switch c {
	case ColourRed, ColourYellow, ColourGreen, ColourBlue:
		return true
	default:
		return false
	}
}

// Value is the face value of a card: "0".."9", or one of the action values.
type Value string

const (
	ValueSkip     Value = "skip"
	ValueReverse  Value = "reverse"
	ValueDrawTwo  Value = "draw2"
	ValueWild     Value = "wild"
	ValueWildDraw Value = "wild4"
)

// Card is a single Uno card. The JSON tags match the state schema agreed
// between the game pod and the proxy.
type Card struct {
	Colour Colour `json:"color"`
	Value  Value  `json:"value"`
}

// String renders a card for logs and test failure messages.
func (c Card) String() string {
	return fmt.Sprintf("%s:%s", c.Colour, c.Value)
}

// IsWild reports whether the card is a Wild or Wild Draw Four.
func (c Card) IsWild() bool {
	return c.Value == ValueWild || c.Value == ValueWildDraw
}

// IsAction reports whether the card has an effect beyond advancing the turn.
func (c Card) IsAction() bool {
	switch c.Value {
	case ValueSkip, ValueReverse, ValueDrawTwo, ValueWild, ValueWildDraw:
		return true
	default:
		return false
	}
}

// numberValues are the number faces in the order they appear in a deck.
var numberValues = []Value{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}

// actionValues are the coloured action faces (wilds are built separately).
var actionValues = []Value{ValueSkip, ValueReverse, ValueDrawTwo}

// NewDeck builds a standard 108-card Uno deck in a fixed, canonical order.
//
// Composition (108 cards):
//   - one 0 per colour                    =  4
//   - two each of 1-9 per colour          = 72
//   - two each of Skip/Reverse/Draw Two   = 24
//   - four Wild                           =  4
//   - four Wild Draw Four                 =  4
//
// The order is deterministic; shuffling is a separate, explicitly seeded step.
func NewDeck() []Card {
	deck := make([]Card, 0, 108)
	for _, colour := range Colours() {
		// Exactly one zero per colour.
		deck = append(deck, Card{Colour: colour, Value: numberValues[0]})
		// Two of each 1-9.
		for _, v := range numberValues[1:] {
			deck = append(deck, Card{Colour: colour, Value: v})
			deck = append(deck, Card{Colour: colour, Value: v})
		}
		// Two of each coloured action card.
		for _, v := range actionValues {
			deck = append(deck, Card{Colour: colour, Value: v})
			deck = append(deck, Card{Colour: colour, Value: v})
		}
	}
	for i := 0; i < 4; i++ {
		deck = append(deck, Card{Colour: ColourWild, Value: ValueWild})
	}
	for i := 0; i < 4; i++ {
		deck = append(deck, Card{Colour: ColourWild, Value: ValueWildDraw})
	}
	return deck
}
