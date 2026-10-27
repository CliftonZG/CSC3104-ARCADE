package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/CSC3104-ARCADE/pkg/protocol"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/coder/websocket"
)

func runRemoteClient(ctx context.Context, wsURL string, playerID string) error {
	fmt.Printf("%s[REMOTE]%s Dialing backend WebSocket at: %s (player: %s)\n", ansiBold, ansiReset, wsURL, playerID)

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}
	defer func() { _ = conn.CloseNow() }()

	fmt.Printf("%s[REMOTE]%s Connected! Waiting for match state from gameserver...\n", ansiGreen, ansiReset)

	reader := bufio.NewReader(os.Stdin)

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("connection closed by server: %w", err)
		}

		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}

		switch env.Type {
		case protocol.TypeStateBroadcast:
			handleRemoteState(reader, conn, env.Payload, playerID)
		case protocol.TypeErrorResponse:
			var errPayload protocol.ErrorPayload
			_ = json.Unmarshal(env.Payload, &errPayload)
			fmt.Printf("%s[SERVER ERROR %s]:%s %s\n", ansiRed, errPayload.Code, ansiReset, errPayload.Message)
		}
	}
}

func handleRemoteState(reader *bufio.Reader, conn *websocket.Conn, rawPayload []byte, myPlayerID string) {
	state, err := uno.RestoreFromSnapshot(rawPayload)
	if err != nil {
		fmt.Printf("%sWarning: received corrupt state: %v%s\n", ansiRed, err, ansiReset)
		return
	}

	cur := state.CurrentPlayer()
	printTableState(state, cur, state.Players, myPlayerID)

	if state.Status == uno.StatusFinished {
		printGameOver(state.Winner, myPlayerID)
		os.Exit(0)
	}

	if cur == myPlayerID {
		promptRemoteMove(reader, conn, state, myPlayerID)
	} else {
		fmt.Printf("Waiting for opponent %s%s%s to move...\n", ansiBold, cur, ansiReset)
	}
}

func promptRemoteMove(reader *bufio.Reader, conn *websocket.Conn, state *uno.State, myPlayerID string) {
	hand := state.Hands[myPlayerID]
	fmt.Printf("\n%s👉 IT'S YOUR TURN!%s Your Hand (%d cards):\n", ansiGreen, ansiReset, len(hand))
	for i, c := range hand {
		playable := ""
		if uno.IsPlayable(state, myPlayerID, c) {
			playable = fmt.Sprintf(" %s(Playable)%s", ansiGreen, ansiReset)
		}
		fmt.Printf("  [%d] %-20s%s\n", i, formatCard(c), playable)
	}

	fmt.Print("\nYour Move [index | 'd' to draw | 'p' to pass | 'q' to quit]: ")
	input, _ := reader.ReadString('\n')
	input = strings.TrimSpace(input)

	if input == "q" {
		fmt.Println("Leaving match.")
		os.Exit(0)
	}

	var act uno.Action
	act.PlayerID = myPlayerID

	switch input {
	case "d", "draw":
		act.Type = uno.ActionDraw
	case "p", "pass":
		act.Type = uno.ActionPass
	default:
		idx, err := strconv.Atoi(input)
		if err != nil || idx < 0 || idx >= len(hand) {
			fmt.Printf("%sInvalid choice.%s\n", ansiRed, ansiReset)
			return
		}
		act.Type = uno.ActionPlay
		act.CardIndex = idx
		if hand[idx].IsWild() {
			act.DeclaredColour = promptDeclaredColor(reader)
		}
	}

	payloadBytes, _ := json.Marshal(act)
	reqEnv := protocol.Envelope{
		Type:      protocol.TypeActionRequest,
		Timestamp: time.Now().UnixMilli(),
		Payload:   payloadBytes,
	}

	envBytes, _ := json.Marshal(reqEnv)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, envBytes)
}
