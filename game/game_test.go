package uno

import (
	"encoding/json"
	"errors"
	"testing"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// newTestState builds a match with controlled, empty hands and a known top
// card, so individual rules can be exercised in isolation rather than through
// a shuffled deck.
func newTestState(t *testing.T, players []string, top Card, colour Colour) *State {
	t.Helper()
	s := &State{
		MatchID:            "test",
		Status:             StatusInProgress,
		Players:            append([]string(nil), players...),
		Hands:              make(map[string][]Card, len(players)),
		Deck:               NewDeck(), // plentiful, deterministic draw pile
		DiscardPile:        []Card{top},
		CurrentColour:      colour,
		CurrentPlayerIndex: 0,
		Direction:          DirectionClockwise,
		Seed:               1,
	}
	for _, p := range players {
		s.Hands[p] = nil
	}
	return s
}

func giveHand(s *State, player string, cards ...Card) {
	s.Hands[player] = append([]Card(nil), cards...)
}

// countCards returns the total number of cards across hands, deck and discard.
// Uno is a closed system: this must equal 108 at every point in a match.
func countCards(s *State) int {
	n := len(s.Deck) + len(s.DiscardPile)
	for _, h := range s.Hands {
		n += len(h)
	}
	return n
}

func mustPlay(t *testing.T, s *State, player string, idx int, colour Colour) *State {
	t.Helper()
	next, err := Play(s, player, idx, colour)
	if err != nil {
		t.Fatalf("Play(%s, %d, %q) returned unexpected error: %v", player, idx, colour, err)
	}
	return next
}

// ---------------------------------------------------------------------------
// deck
// ---------------------------------------------------------------------------

func TestNewDeckHas108Cards(t *testing.T) {
	if got := len(NewDeck()); got != 108 {
		t.Fatalf("deck size = %d, want 108", got)
	}
}

func TestNewDeckComposition(t *testing.T) {
	counts := make(map[Card]int)
	for _, c := range NewDeck() {
		counts[c]++
	}

	for _, colour := range Colours() {
		if got := counts[Card{Colour: colour, Value: "0"}]; got != 1 {
			t.Errorf("%s 0: got %d, want 1", colour, got)
		}
		for _, v := range []Value{"1", "2", "3", "4", "5", "6", "7", "8", "9"} {
			if got := counts[Card{Colour: colour, Value: v}]; got != 2 {
				t.Errorf("%s %s: got %d, want 2", colour, v, got)
			}
		}
		for _, v := range []Value{ValueSkip, ValueReverse, ValueDrawTwo} {
			if got := counts[Card{Colour: colour, Value: v}]; got != 2 {
				t.Errorf("%s %s: got %d, want 2", colour, v, got)
			}
		}
	}

	if got := counts[Card{Colour: ColourWild, Value: ValueWild}]; got != 4 {
		t.Errorf("wild: got %d, want 4", got)
	}
	if got := counts[Card{Colour: ColourWild, Value: ValueWildDraw}]; got != 4 {
		t.Errorf("wild draw four: got %d, want 4", got)
	}
}

func TestShuffleIsDeterministicForAGivenSeed(t *testing.T) {
	a, b, c := NewDeck(), NewDeck(), NewDeck()
	shuffleDeterministic(a, 42)
	shuffleDeterministic(b, 42)
	shuffleDeterministic(c, 43)

	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("same seed produced different decks at index %d: %s vs %s", i, a[i], b[i])
		}
	}

	same := true
	for i := range a {
		if a[i] != c[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different seeds produced an identical deck ordering")
	}
}

func TestShufflePreservesComposition(t *testing.T) {
	deck := NewDeck()
	shuffleDeterministic(deck, 7)

	counts := make(map[Card]int)
	for _, c := range deck {
		counts[c]++
	}
	if len(deck) != 108 {
		t.Fatalf("shuffled deck size = %d, want 108", len(deck))
	}
	if counts[Card{Colour: ColourWild, Value: ValueWildDraw}] != 4 {
		t.Error("shuffle changed the number of wild draw fours")
	}
}

// ---------------------------------------------------------------------------
// game setup
// ---------------------------------------------------------------------------

func TestNewGameDealsCorrectly(t *testing.T) {
	players := []string{"p1", "p2", "p3"}
	s, err := NewGame("m1", players, 99)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}

	for _, p := range players {
		if got := len(s.Hands[p]); got != HandSize {
			t.Errorf("player %s hand size = %d, want %d", p, got, HandSize)
		}
	}
	if len(s.DiscardPile) != 1 {
		t.Errorf("discard pile size = %d, want 1", len(s.DiscardPile))
	}
	if got := countCards(s); got != 108 {
		t.Errorf("total cards after deal = %d, want 108", got)
	}
	if s.Status != StatusInProgress {
		t.Errorf("status = %q, want %q", s.Status, StatusInProgress)
	}
	if s.Direction != DirectionClockwise {
		t.Errorf("direction = %d, want %d", s.Direction, DirectionClockwise)
	}
	if s.Sequence != 0 {
		t.Errorf("sequence = %d, want 0", s.Sequence)
	}
}

// The starting discard must be a plain number card, so no action effect is
// pending before the first player has acted.
func TestNewGameStartCardIsANumberCard(t *testing.T) {
	for seed := int64(0); seed < 300; seed++ {
		s, err := NewGame("m", []string{"p1", "p2"}, seed)
		if err != nil {
			t.Fatalf("seed %d: NewGame returned error: %v", seed, err)
		}
		top, ok := s.TopCard()
		if !ok {
			t.Fatalf("seed %d: discard pile is empty", seed)
		}
		if top.IsAction() {
			t.Fatalf("seed %d: start card %s is an action card", seed, top)
		}
		if top.Colour != s.CurrentColour {
			t.Fatalf("seed %d: colour in force %q does not match start card %s",
				seed, s.CurrentColour, top)
		}
	}
}

func TestNewGameIsReproducibleFromSeed(t *testing.T) {
	a, err := NewGame("m", []string{"p1", "p2", "p3"}, 2024)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}
	b, err := NewGame("m", []string{"p1", "p2", "p3"}, 2024)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}

	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatal("two games built from the same seed differ")
	}
}

func TestNewGameRejectsBadPlayerLists(t *testing.T) {
	tooMany := make([]string, MaxPlayers+1)
	for i := range tooMany {
		tooMany[i] = string(rune('a' + i))
	}

	cases := []struct {
		name    string
		players []string
		want    error
	}{
		{"one player", []string{"p1"}, ErrTooFewPlayers},
		{"no players", nil, ErrTooFewPlayers},
		{"duplicate", []string{"p1", "p1"}, ErrDuplicatePlayer},
		{"empty id", []string{"p1", ""}, ErrEmptyPlayerID},
		{"too many", tooMany, ErrTooManyPlayers},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewGame("m", tc.players, 1)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// basic play and turn order
// ---------------------------------------------------------------------------

func TestPlayNumberCardAdvancesTurn(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2", "p3"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "9"}, Card{ColourBlue, "3"})

	next := mustPlay(t, s, "p1", 0, "")

	if next.CurrentPlayer() != "p2" {
		t.Errorf("current player = %q, want p2", next.CurrentPlayer())
	}
	if next.CurrentColour != ColourRed {
		t.Errorf("colour in force = %q, want red", next.CurrentColour)
	}
	top, _ := next.TopCard()
	if top != (Card{ColourRed, "9"}) {
		t.Errorf("top card = %s, want red:9", top)
	}
	if got := len(next.Hands["p1"]); got != 1 {
		t.Errorf("hand size after play = %d, want 1", got)
	}
	if next.Sequence != s.Sequence+1 {
		t.Errorf("sequence = %d, want %d", next.Sequence, s.Sequence+1)
	}
}

func TestPlayMatchesOnValueAcrossColours(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "7"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "7"})

	next := mustPlay(t, s, "p1", 0, "")
	if next.CurrentColour != ColourBlue {
		t.Errorf("colour in force = %q, want blue", next.CurrentColour)
	}
}

func TestPlayRejectsNonMatchingCard(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "7"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "3"})

	if _, err := Play(s, "p1", 0, ""); !errors.Is(err, ErrIllegalPlay) {
		t.Fatalf("error = %v, want ErrIllegalPlay", err)
	}
}

func TestPlayRejectsOutOfTurn(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "7"}, ColourRed)
	giveHand(s, "p2", Card{ColourRed, "3"})

	if _, err := Play(s, "p2", 0, ""); !errors.Is(err, ErrNotYourTurn) {
		t.Fatalf("error = %v, want ErrNotYourTurn", err)
	}
}

func TestPlayRejectsUnknownPlayerAndBadIndex(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "7"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "3"})

	if _, err := Play(s, "ghost", 0, ""); !errors.Is(err, ErrUnknownPlayer) {
		t.Fatalf("error = %v, want ErrUnknownPlayer", err)
	}
	if _, err := Play(s, "p1", 5, ""); !errors.Is(err, ErrCardIndex) {
		t.Fatalf("error = %v, want ErrCardIndex", err)
	}
	if _, err := Play(s, "p1", -1, ""); !errors.Is(err, ErrCardIndex) {
		t.Fatalf("error = %v, want ErrCardIndex", err)
	}
}

// A rejected action must not modify the caller's state.
func TestRejectedPlayLeavesStateUntouched(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "7"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "3"})
	before, _ := json.Marshal(s)

	if _, err := Play(s, "p1", 0, ""); err == nil {
		t.Fatal("expected the play to be rejected")
	}

	after, _ := json.Marshal(s)
	if string(before) != string(after) {
		t.Fatal("a rejected play mutated the input state")
	}
}

// ---------------------------------------------------------------------------
// action cards
// ---------------------------------------------------------------------------

func TestSkipJumpsOnePlayer(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2", "p3"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, ValueSkip}, Card{ColourBlue, "1"})

	next := mustPlay(t, s, "p1", 0, "")
	if next.CurrentPlayer() != "p3" {
		t.Errorf("current player = %q, want p3 (p2 skipped)", next.CurrentPlayer())
	}
}

func TestReverseFlipsDirection(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2", "p3"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, ValueReverse}, Card{ColourBlue, "1"})

	next := mustPlay(t, s, "p1", 0, "")
	if next.Direction != DirectionCounterClockwise {
		t.Errorf("direction = %d, want %d", next.Direction, DirectionCounterClockwise)
	}
	if next.CurrentPlayer() != "p3" {
		t.Errorf("current player = %q, want p3 (reversed from p1)", next.CurrentPlayer())
	}
}

// With two players a Reverse acts as a Skip, returning the turn to the player
// who played it. This is the official two-player rule.
func TestReverseActsAsSkipWithTwoPlayers(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, ValueReverse}, Card{ColourBlue, "1"})

	next := mustPlay(t, s, "p1", 0, "")
	if next.CurrentPlayer() != "p1" {
		t.Errorf("current player = %q, want p1", next.CurrentPlayer())
	}
	if next.Direction != DirectionCounterClockwise {
		t.Errorf("direction = %d, want flipped", next.Direction)
	}
}

func TestDrawTwoPenalisesAndSkipsNextPlayer(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2", "p3"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, ValueDrawTwo}, Card{ColourBlue, "1"})
	giveHand(s, "p2", Card{ColourGreen, "4"})

	next := mustPlay(t, s, "p1", 0, "")

	if got := len(next.Hands["p2"]); got != 3 {
		t.Errorf("p2 hand size = %d, want 3 (1 + 2 drawn)", got)
	}
	if next.CurrentPlayer() != "p3" {
		t.Errorf("current player = %q, want p3 (p2 skipped)", next.CurrentPlayer())
	}
}

func TestWildSetsDeclaredColour(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourWild, ValueWild}, Card{ColourBlue, "1"})

	next := mustPlay(t, s, "p1", 0, ColourGreen)

	if next.CurrentColour != ColourGreen {
		t.Errorf("colour in force = %q, want green", next.CurrentColour)
	}
	// The card itself stays colourless on the pile.
	top, _ := next.TopCard()
	if top.Colour != ColourWild {
		t.Errorf("top card colour = %q, want wild", top.Colour)
	}
	if next.CurrentPlayer() != "p2" {
		t.Errorf("current player = %q, want p2", next.CurrentPlayer())
	}
}

func TestWildRequiresAValidColour(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourWild, ValueWild})

	if _, err := Play(s, "p1", 0, ""); !errors.Is(err, ErrColourRequired) {
		t.Fatalf("error = %v, want ErrColourRequired", err)
	}
	if _, err := Play(s, "p1", 0, ColourWild); !errors.Is(err, ErrColourRequired) {
		t.Fatalf("declaring 'wild' as a colour: error = %v, want ErrColourRequired", err)
	}
	if _, err := Play(s, "p1", 0, "purple"); !errors.Is(err, ErrColourRequired) {
		t.Fatalf("invalid colour: error = %v, want ErrColourRequired", err)
	}
}

func TestNonWildRejectsDeclaredColour(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "9"})

	if _, err := Play(s, "p1", 0, ColourBlue); !errors.Is(err, ErrColourNotAllowed) {
		t.Fatalf("error = %v, want ErrColourNotAllowed", err)
	}
}

func TestWildDrawFourLegalWhenNoColourMatch(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2", "p3"}, Card{ColourRed, "5"}, ColourRed)
	// No red card in hand, so the wild draw four is legal.
	giveHand(s, "p1", Card{ColourWild, ValueWildDraw}, Card{ColourBlue, "1"})
	giveHand(s, "p2", Card{ColourGreen, "4"})

	next := mustPlay(t, s, "p1", 0, ColourYellow)

	if next.CurrentColour != ColourYellow {
		t.Errorf("colour in force = %q, want yellow", next.CurrentColour)
	}
	if got := len(next.Hands["p2"]); got != 5 {
		t.Errorf("p2 hand size = %d, want 5 (1 + 4 drawn)", got)
	}
	if next.CurrentPlayer() != "p3" {
		t.Errorf("current player = %q, want p3 (p2 skipped)", next.CurrentPlayer())
	}
}

func TestWildDrawFourIllegalWhileHoldingCurrentColour(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourWild, ValueWildDraw}, Card{ColourRed, "1"})

	if _, err := Play(s, "p1", 0, ColourYellow); !errors.Is(err, ErrWildDrawFour) {
		t.Fatalf("error = %v, want ErrWildDrawFour", err)
	}
}

// ---------------------------------------------------------------------------
// drawing and passing
// ---------------------------------------------------------------------------

func TestDrawUnplayableCardEndsTurn(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})
	// Force the next drawn card to be unplayable on red:5.
	s.Deck = []Card{{ColourGreen, "9"}}

	next, err := Draw(s, "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}
	if got := len(next.Hands["p1"]); got != 2 {
		t.Errorf("hand size = %d, want 2", got)
	}
	if next.CurrentPlayer() != "p2" {
		t.Errorf("current player = %q, want p2 (turn should end)", next.CurrentPlayer())
	}
	if next.DrewThisTurn {
		t.Error("DrewThisTurn should reset when the turn advances")
	}
}

func TestDrawPlayableCardKeepsTurn(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})
	s.Deck = []Card{{ColourRed, "9"}}

	next, err := Draw(s, "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}
	if next.CurrentPlayer() != "p1" {
		t.Errorf("current player = %q, want p1 (drawn card is playable)", next.CurrentPlayer())
	}
	if !next.DrewThisTurn {
		t.Error("DrewThisTurn should be set")
	}

	// The player may now play the card they drew.
	after := mustPlay(t, next, "p1", 1, "")
	if after.CurrentPlayer() != "p2" {
		t.Errorf("current player = %q, want p2", after.CurrentPlayer())
	}
}

func TestCannotDrawTwiceInOneTurn(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})
	s.Deck = []Card{{ColourRed, "9"}, {ColourRed, "3"}}

	next, err := Draw(s, "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}
	if _, err := Draw(next, "p1"); !errors.Is(err, ErrAlreadyDrew) {
		t.Fatalf("error = %v, want ErrAlreadyDrew", err)
	}
}

func TestPassRequiresADrawFirst(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})

	if _, err := Pass(s, "p1"); !errors.Is(err, ErrMustDrawFirst) {
		t.Fatalf("error = %v, want ErrMustDrawFirst", err)
	}
}

func TestPassAfterDrawAdvancesTurn(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})
	s.Deck = []Card{{ColourRed, "9"}}

	drawn, err := Draw(s, "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}
	next, err := Pass(drawn, "p1")
	if err != nil {
		t.Fatalf("Pass returned error: %v", err)
	}
	if next.CurrentPlayer() != "p2" {
		t.Errorf("current player = %q, want p2", next.CurrentPlayer())
	}
}

// ---------------------------------------------------------------------------
// reshuffling
// ---------------------------------------------------------------------------

func TestDrawReshufflesDiscardWhenDeckIsEmpty(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})
	s.Deck = nil
	s.DiscardPile = []Card{
		{ColourGreen, "2"}, {ColourYellow, "8"}, {ColourBlue, "4"}, {ColourRed, "5"},
	}

	next, err := Draw(s, "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}
	if next.ReshuffleCount != 1 {
		t.Errorf("reshuffle count = %d, want 1", next.ReshuffleCount)
	}
	if len(next.DiscardPile) != 1 {
		t.Errorf("discard pile size = %d, want 1 (top card retained)", len(next.DiscardPile))
	}
	if top, _ := next.TopCard(); top != (Card{ColourRed, "5"}) {
		t.Errorf("top card = %s, want red:5 to be preserved", top)
	}
	// Three recycled cards, one of which went into the hand.
	if got := len(next.Deck); got != 2 {
		t.Errorf("deck size = %d, want 2", got)
	}
}

// A reshuffle must be reproducible from the snapshot alone, otherwise a
// promoted backup would deal different cards than the crashed primary.
func TestReshuffleIsDeterministic(t *testing.T) {
	build := func() *State {
		s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
		giveHand(s, "p1", Card{ColourBlue, "1"})
		s.Seed = 555
		s.Deck = nil
		s.DiscardPile = []Card{
			{ColourGreen, "2"}, {ColourYellow, "8"}, {ColourBlue, "4"},
			{ColourGreen, "7"}, {ColourYellow, "1"}, {ColourRed, "5"},
		}
		return s
	}

	a, err := Draw(build(), "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}
	b, err := Draw(build(), "p1")
	if err != nil {
		t.Fatalf("Draw returned error: %v", err)
	}

	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatal("two reshuffles from identical snapshots produced different results")
	}
}

// A wild card returning to the deck must lose the colour that was declared
// when it was played, otherwise it would re-enter play as a coloured card.
func TestReshuffleResetsWildColour(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourBlue, "1"})
	s.Deck = nil
	s.DiscardPile = []Card{
		{ColourGreen, ValueWild}, // a wild that was played as green
		{ColourBlue, "4"},
		{ColourRed, "5"},
	}

	reshuffleDiscard(s)

	for _, c := range s.Deck {
		if c.IsWild() && c.Colour != ColourWild {
			t.Fatalf("recycled wild kept its declared colour: %s", c)
		}
	}
}

// ---------------------------------------------------------------------------
// winning
// ---------------------------------------------------------------------------

func TestPlayingLastCardWinsTheGame(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "9"})

	next := mustPlay(t, s, "p1", 0, "")

	if next.Status != StatusFinished {
		t.Errorf("status = %q, want %q", next.Status, StatusFinished)
	}
	if next.Winner != "p1" {
		t.Errorf("winner = %q, want p1", next.Winner)
	}
}

// A Draw Two played as the final card still penalises the next player.
func TestWinningWithDrawTwoStillPenalisesNextPlayer(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2", "p3"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, ValueDrawTwo})
	giveHand(s, "p2", Card{ColourGreen, "4"})

	next := mustPlay(t, s, "p1", 0, "")

	if next.Winner != "p1" {
		t.Fatalf("winner = %q, want p1", next.Winner)
	}
	if got := len(next.Hands["p2"]); got != 3 {
		t.Errorf("p2 hand size = %d, want 3", got)
	}
}

func TestNoActionsAfterGameOver(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "9"})
	finished := mustPlay(t, s, "p1", 0, "")

	if _, err := Play(finished, "p2", 0, ""); !errors.Is(err, ErrGameOver) {
		t.Errorf("Play after game over: error = %v, want ErrGameOver", err)
	}
	if _, err := Draw(finished, "p2"); !errors.Is(err, ErrGameOver) {
		t.Errorf("Draw after game over: error = %v, want ErrGameOver", err)
	}
	if _, err := Pass(finished, "p2"); !errors.Is(err, ErrGameOver) {
		t.Errorf("Pass after game over: error = %v, want ErrGameOver", err)
	}
}

// ---------------------------------------------------------------------------
// state handling
// ---------------------------------------------------------------------------

func TestCloneIsDeep(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "9"})

	c := s.Clone()
	c.Hands["p1"][0] = Card{ColourBlue, "1"}
	c.Players[0] = "changed"
	c.Deck = append(c.Deck, Card{ColourBlue, "2"})

	if s.Hands["p1"][0] != (Card{ColourRed, "9"}) {
		t.Error("mutating the clone's hand changed the original")
	}
	if s.Players[0] != "p1" {
		t.Error("mutating the clone's player list changed the original")
	}
}

func TestHandReturnsACopy(t *testing.T) {
	s := newTestState(t, []string{"p1", "p2"}, Card{ColourRed, "5"}, ColourRed)
	giveHand(s, "p1", Card{ColourRed, "9"})

	hand, ok := s.Hand("p1")
	if !ok {
		t.Fatal("Hand returned ok = false for a known player")
	}
	hand[0] = Card{ColourBlue, "1"}

	if s.Hands["p1"][0] != (Card{ColourRed, "9"}) {
		t.Error("mutating the returned hand changed the state")
	}
}

func TestStateSurvivesJSONRoundTrip(t *testing.T) {
	s, err := NewGame("m1", []string{"p1", "p2", "p3"}, 31337)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}
	s = mustPlayFirstLegalMove(t, s)

	data, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var restored State
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	again, err := json.Marshal(&restored)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	if string(data) != string(again) {
		t.Fatal("state changed across a JSON round trip")
	}
}

// ---------------------------------------------------------------------------
// crash recovery: the property the whole system depends on
// ---------------------------------------------------------------------------

// chooseColour picks a declared colour deterministically: the colour the
// player holds most of, with ties broken by the fixed order in Colours().
func chooseColour(s *State, playerID string) Colour {
	counts := make(map[Colour]int)
	for _, c := range s.Hands[playerID] {
		counts[c.Colour]++
	}
	best, bestN := ColourRed, -1
	for _, col := range Colours() {
		if counts[col] > bestN {
			best, bestN = col, counts[col]
		}
	}
	return best
}

// stepOnce applies one deterministic turn: play the first legal card, else
// draw, else pass. It returns the next state.
func stepOnce(t *testing.T, s *State) *State {
	t.Helper()
	player := s.CurrentPlayer()

	for i, c := range s.Hands[player] {
		if !IsPlayable(s, player, c) {
			continue
		}
		colour := Colour("")
		if c.IsWild() {
			colour = chooseColour(s, player)
		}
		next, err := Play(s, player, i, colour)
		if err != nil {
			t.Fatalf("playing %s for %s failed: %v", c, player, err)
		}
		return next
	}

	if !s.DrewThisTurn {
		next, err := Draw(s, player)
		if err != nil {
			t.Fatalf("draw for %s failed: %v", player, err)
		}
		return next
	}

	next, err := Pass(s, player)
	if err != nil {
		t.Fatalf("pass for %s failed: %v", player, err)
	}
	return next
}

func mustPlayFirstLegalMove(t *testing.T, s *State) *State {
	t.Helper()
	return stepOnce(t, s)
}

// This is the behaviour ARCADE's failover depends on: a pod that reloads a
// snapshot and replays the same moves must reach a bit-identical state to the
// pod that crashed. If the engine consulted live randomness anywhere, this
// test would fail.
func TestResumeFromSnapshotMatchesUninterruptedPlay(t *testing.T) {
	const crashAfter = 25
	const totalMoves = 120

	original, err := NewGame("m1", []string{"p1", "p2", "p3", "p4"}, 8675309)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}

	// Play up to the crash point.
	live := original
	for i := 0; i < crashAfter && live.Status == StatusInProgress; i++ {
		live = stepOnce(t, live)
	}

	// Snapshot exactly as the primary would write to Redis.
	snapshot, err := json.Marshal(live)
	if err != nil {
		t.Fatalf("Marshal returned error: %v", err)
	}
	var recovered State
	if err := json.Unmarshal(snapshot, &recovered); err != nil {
		t.Fatalf("Unmarshal returned error: %v", err)
	}

	// Continue both the surviving state and the recovered one.
	a, b := live, &recovered
	for i := crashAfter; i < totalMoves; i++ {
		if a.Status != StatusInProgress {
			break
		}
		a = stepOnce(t, a)
		b = stepOnce(t, b)
	}

	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) != string(jb) {
		t.Fatal("state recovered from a snapshot diverged from uninterrupted play")
	}
}

// Cards are never created or destroyed, including across reshuffles.
func TestCardCountIsConservedThroughoutAMatch(t *testing.T) {
	s, err := NewGame("m1", []string{"p1", "p2", "p3"}, 4242)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}

	for i := 0; i < 400 && s.Status == StatusInProgress; i++ {
		s = stepOnce(t, s)
		if got := countCards(s); got != 108 {
			t.Fatalf("after move %d: card count = %d, want 108", i+1, got)
		}
	}
}

// The sequence number must advance on every accepted action, since the
// control plane uses it to decide whether a backup is up to date.
func TestSequenceIncrementsOnEveryAction(t *testing.T) {
	s, err := NewGame("m1", []string{"p1", "p2"}, 97)
	if err != nil {
		t.Fatalf("NewGame returned error: %v", err)
	}

	prev := s.Sequence
	for i := 0; i < 60 && s.Status == StatusInProgress; i++ {
		s = stepOnce(t, s)
		if s.Sequence != prev+1 {
			t.Fatalf("after move %d: sequence = %d, want %d", i+1, s.Sequence, prev+1)
		}
		prev = s.Sequence
	}
}

// Games must actually terminate; a rules bug that left no legal move would
// show up here as a match that never finishes.
func TestMatchesReachAWinner(t *testing.T) {
	for seed := int64(0); seed < 40; seed++ {
		s, err := NewGame("m", []string{"p1", "p2", "p3"}, seed)
		if err != nil {
			t.Fatalf("seed %d: NewGame returned error: %v", seed, err)
		}
		moves := 0
		for s.Status == StatusInProgress && moves < 5000 {
			s = stepOnce(t, s)
			moves++
		}
		if s.Status != StatusFinished {
			t.Fatalf("seed %d: no winner after %d moves", seed, moves)
		}
		if s.Winner == "" {
			t.Fatalf("seed %d: finished with an empty winner", seed)
		}
		if len(s.Hands[s.Winner]) != 0 {
			t.Fatalf("seed %d: winner %q still holds cards", seed, s.Winner)
		}
	}
}
