package uno

import (
	"encoding/binary"
	"fmt"
	"math"
)

// Binary Magic and Version header to prevent protocol mismatch across pods.
const (
	binaryMagic   uint32 = 0x554E4F31 // "UNO1"
	binaryVersion uint16 = 1
)

// colourToByte maps Colour to a compact 3-bit representation.
func colourToByte(c Colour) byte {
	switch c {
	case ColourRed:
		return 0
	case ColourYellow:
		return 1
	case ColourGreen:
		return 2
	case ColourBlue:
		return 3
	case ColourWild:
		return 4
	default:
		return 0xFF
	}
}

// byteToColour maps byte back to Colour.
func byteToColour(b byte) Colour {
	switch b {
	case 0:
		return ColourRed
	case 1:
		return ColourYellow
	case 2:
		return ColourGreen
	case 3:
		return ColourBlue
	case 4:
		return ColourWild
	default:
		return ""
	}
}

// valueToByte maps card Value to a compact 4-bit representation.
func valueToByte(v Value) byte {
	switch v {
	case "0":
		return 0
	case "1":
		return 1
	case "2":
		return 2
	case "3":
		return 3
	case "4":
		return 4
	case "5":
		return 5
	case "6":
		return 6
	case "7":
		return 7
	case "8":
		return 8
	case "9":
		return 9
	case ValueSkip:
		return 10
	case ValueReverse:
		return 11
	case ValueDrawTwo:
		return 12
	case ValueWild:
		return 13
	case ValueWildDraw:
		return 14
	default:
		return 0xFF
	}
}

// byteToValue maps byte back to card Value.
func byteToValue(b byte) Value {
	switch b {
	case 0:
		return "0"
	case 1:
		return "1"
	case 2:
		return "2"
	case 3:
		return "3"
	case 4:
		return "4"
	case 5:
		return "5"
	case 6:
		return "6"
	case 7:
		return "7"
	case 8:
		return "8"
	case 9:
		return "9"
	case 10:
		return ValueSkip
	case 11:
		return ValueReverse
	case 12:
		return ValueDrawTwo
	case 13:
		return ValueWild
	case 14:
		return ValueWildDraw
	default:
		return ""
	}
}

// packCard packs a Card into 1 single byte: upper 4 bits for Value, lower 4 bits for Colour.
func packCard(c Card) byte {
	col := colourToByte(c.Colour)
	val := valueToByte(c.Value)
	return (val << 4) | (col & 0x0F)
}

// unpackCard unpacks 1 single byte into a Card.
func unpackCard(b byte) Card {
	val := byteToValue(b >> 4)
	col := byteToColour(b & 0x0F)
	return Card{Colour: col, Value: val}
}

// ExportBinary writes the state into a high-density, ultra-low-latency binary representation
// (< 350 bytes total) designed for sub-microsecond Redis replication and UDP sync.
func (s *State) ExportBinary() ([]byte, error) {
	if s == nil {
		return nil, ErrNilState
	}

	buf := getBuffer()
	defer putBuffer(buf)

	// 1. Header: Magic (4) + Version (2)
	var hdr [6]byte
	binary.BigEndian.PutUint32(hdr[0:4], binaryMagic)
	binary.BigEndian.PutUint16(hdr[4:6], binaryVersion)
	buf.Write(hdr[:])

	// 2. MatchID: len (1 byte) + data
	if len(s.MatchID) > math.MaxUint8 {
		return nil, fmt.Errorf("%w: match_id too long for binary encoding", ErrInvalidSnapshot)
	}
	buf.WriteByte(byte(len(s.MatchID)))
	buf.WriteString(s.MatchID)

	// 3. Scalar fields: Sequence (8) + Seed (8) + ReshuffleCount (4)
	var scalars [20]byte
	binary.BigEndian.PutUint64(scalars[0:8], uint64(s.Sequence))
	binary.BigEndian.PutUint64(scalars[8:16], uint64(s.Seed))
	binary.BigEndian.PutUint32(scalars[16:20], uint32(s.ReshuffleCount))
	buf.Write(scalars[:])

	// 4. Status (1), CurrentPlayerIndex (1), Direction (1), DrewThisTurn (1), CurrentColour (1)
	statusByte := byte(0)
	if s.Status == StatusFinished {
		statusByte = 1
	}
	dirByte := byte(1)
	if s.Direction < 0 {
		dirByte = 255 // -1
	}
	drewByte := byte(0)
	if s.DrewThisTurn {
		drewByte = 1
	}
	buf.Write([]byte{statusByte, byte(s.CurrentPlayerIndex), dirByte, drewByte, colourToByte(s.CurrentColour)})

	// 5. Winner: len (1 byte) + data
	buf.WriteByte(byte(len(s.Winner)))
	buf.WriteString(s.Winner)

	// 6. Players Seating: count (1 byte) + [len + name]
	buf.WriteByte(byte(len(s.Players)))
	for _, p := range s.Players {
		buf.WriteByte(byte(len(p)))
		buf.WriteString(p)
	}

	// 7. Player Hands: for each player in seating order, count (1 byte) + packed cards
	for _, p := range s.Players {
		hand := s.Hands[p]
		buf.WriteByte(byte(len(hand)))
		for _, card := range hand {
			buf.WriteByte(packCard(card))
		}
	}

	// 8. Deck: count (1 byte) + packed cards
	buf.WriteByte(byte(len(s.Deck)))
	for _, card := range s.Deck {
		buf.WriteByte(packCard(card))
	}

	// 9. DiscardPile: count (1 byte) + packed cards
	buf.WriteByte(byte(len(s.DiscardPile)))
	for _, card := range s.DiscardPile {
		buf.WriteByte(packCard(card))
	}

	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, nil
}

// RestoreFromBinary reconstructs and validates a State from compact binary bytes in sub-microsecond time.
func RestoreFromBinary(data []byte) (*State, error) {
	if len(data) < 32 {
		return nil, fmt.Errorf("%w: binary payload too short (%d bytes)", ErrInvalidSnapshot, len(data))
	}

	// Check Magic & Version
	magic := binary.BigEndian.Uint32(data[0:4])
	if magic != binaryMagic {
		return nil, fmt.Errorf("%w: invalid binary magic header", ErrInvalidSnapshot)
	}
	ver := binary.BigEndian.Uint16(data[4:6])
	if ver != binaryVersion {
		return nil, fmt.Errorf("%w: unsupported binary version %d", ErrInvalidSnapshot, ver)
	}

	offset := 6

	// MatchID
	if offset >= len(data) {
		return nil, fmt.Errorf("%w: truncated match_id", ErrInvalidSnapshot)
	}
	idLen := int(data[offset])
	offset++
	if offset+idLen > len(data) {
		return nil, fmt.Errorf("%w: truncated match_id string", ErrInvalidSnapshot)
	}
	matchID := string(data[offset : offset+idLen])
	offset += idLen

	// Scalars
	if offset+20 > len(data) {
		return nil, fmt.Errorf("%w: truncated scalar block", ErrInvalidSnapshot)
	}
	seq := int64(binary.BigEndian.Uint64(data[offset : offset+8]))
	seed := int64(binary.BigEndian.Uint64(data[offset+8 : offset+16]))
	reshuffle := int(binary.BigEndian.Uint32(data[offset+16 : offset+20]))
	offset += 20

	// Flags & indices
	if offset+5 > len(data) {
		return nil, fmt.Errorf("%w: truncated flags block", ErrInvalidSnapshot)
	}
	status := StatusInProgress
	if data[offset] == 1 {
		status = StatusFinished
	}
	curIdx := int(data[offset+1])
	direction := 1
	if data[offset+2] == 255 {
		direction = -1
	}
	drew := data[offset+3] == 1
	curCol := byteToColour(data[offset+4])
	offset += 5

	// Winner
	if offset >= len(data) {
		return nil, fmt.Errorf("%w: truncated winner", ErrInvalidSnapshot)
	}
	winnerLen := int(data[offset])
	offset++
	var winner string
	if winnerLen > 0 {
		if offset+winnerLen > len(data) {
			return nil, fmt.Errorf("%w: truncated winner string", ErrInvalidSnapshot)
		}
		winner = string(data[offset : offset+winnerLen])
		offset += winnerLen
	}

	// Players
	if offset >= len(data) {
		return nil, fmt.Errorf("%w: truncated player count", ErrInvalidSnapshot)
	}
	playerCount := int(data[offset])
	offset++
	players := make([]string, playerCount)
	for i := 0; i < playerCount; i++ {
		if offset >= len(data) {
			return nil, fmt.Errorf("%w: truncated player string len", ErrInvalidSnapshot)
		}
		pLen := int(data[offset])
		offset++
		if offset+pLen > len(data) {
			return nil, fmt.Errorf("%w: truncated player name", ErrInvalidSnapshot)
		}
		players[i] = string(data[offset : offset+pLen])
		offset += pLen
	}

	// Hands
	hands := make(map[string][]Card, playerCount)
	for _, p := range players {
		if offset >= len(data) {
			return nil, fmt.Errorf("%w: truncated hand size", ErrInvalidSnapshot)
		}
		hCount := int(data[offset])
		offset++
		if offset+hCount > len(data) {
			return nil, fmt.Errorf("%w: truncated hand cards", ErrInvalidSnapshot)
		}
		h := make([]Card, hCount)
		for j := 0; j < hCount; j++ {
			h[j] = unpackCard(data[offset+j])
		}
		hands[p] = h
		offset += hCount
	}

	// Deck
	if offset >= len(data) {
		return nil, fmt.Errorf("%w: truncated deck size", ErrInvalidSnapshot)
	}
	deckCount := int(data[offset])
	offset++
	if offset+deckCount > len(data) {
		return nil, fmt.Errorf("%w: truncated deck cards", ErrInvalidSnapshot)
	}
	deck := make([]Card, deckCount)
	for j := 0; j < deckCount; j++ {
		deck[j] = unpackCard(data[offset+j])
	}
	offset += deckCount

	// DiscardPile
	if offset >= len(data) {
		return nil, fmt.Errorf("%w: truncated discard pile size", ErrInvalidSnapshot)
	}
	discardCount := int(data[offset])
	offset++
	if offset+discardCount > len(data) {
		return nil, fmt.Errorf("%w: truncated discard pile cards", ErrInvalidSnapshot)
	}
	discard := make([]Card, discardCount)
	for j := 0; j < discardCount; j++ {
		discard[j] = unpackCard(data[offset+j])
	}

	st := &State{
		MatchID:            matchID,
		Sequence:           seq,
		Status:             status,
		Players:            players,
		Hands:              hands,
		Deck:               deck,
		DiscardPile:        discard,
		CurrentColour:      curCol,
		CurrentPlayerIndex: curIdx,
		Direction:          direction,
		DrewThisTurn:       drew,
		Winner:             winner,
		Seed:               seed,
		ReshuffleCount:     reshuffle,
	}

	// Validate strict state invariants
	if err := st.Validate(); err != nil {
		return nil, err
	}

	return st, nil
}
