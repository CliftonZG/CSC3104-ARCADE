package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/CSC3104-ARCADE/pkg/protocol"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/coder/websocket"
)

// runRemoteClient joins a match through a proxy (or a gameserver) and plays it.
// If the connection is lost, for example because the proxy it went through
// crashed, it reconnects to the same URL by itself (see reconnect.go) and the
// server replays the current match state; the player does not have to restart
// the client. The player is identified by the URL (?player=<id>), so keep it
// the same.
func runRemoteClient(ctx context.Context, wsURL string, playerID string) error {
	fmt.Printf("%s[REMOTE]%s Dialing backend WebSocket at: %s (player: %s)\n", ansiBold, ansiReset, wsURL, playerID)

	reader := bufio.NewReader(os.Stdin)

	dial := func(ctx context.Context) (*websocket.Conn, *http.Response, error) {
		dctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		conn, resp, err := websocket.Dial(dctx, wsURL, nil)
		if err == nil {
			fmt.Printf("%s[REMOTE]%s Connected! Waiting for match state from gameserver...\n", ansiGreen, ansiReset)
		}
		return conn, resp, err
	}
	play := func(ctx context.Context, conn *websocket.Conn) error {
		return playRemote(ctx, conn, reader, playerID)
	}
	logf := func(format string, args ...any) {
		fmt.Printf("%s[REMOTE]%s %s\n", ansiYellow, ansiReset, fmt.Sprintf(format, args...))
	}
	return reconnectLoop(ctx, defaultRetry, dial, play, logf, sleepCtx)
}

// playRemote reads server messages on one connection until it breaks and
// returns the error that ended it.
func playRemote(ctx context.Context, conn *websocket.Conn, reader *bufio.Reader, playerID string) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			return err
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
