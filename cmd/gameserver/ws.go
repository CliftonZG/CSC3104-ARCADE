package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/protocol"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/coder/websocket"
)

// GameServerWS manages client WebSocket connections for the active match.
type GameServerWS struct {
	status  *fleet.Status
	getGame func() *uno.Game
	mu      sync.Mutex
	conns   map[*websocket.Conn]string
	counter atomic.Int64 // Handles "inc" messages from test clients
}

func NewGameServerWS(status *fleet.Status, getGame func() *uno.Game) *GameServerWS {
	return &GameServerWS{
		status:  status,
		getGame: getGame,
		conns:   make(map[*websocket.Conn]string),
	}
}

// ServeHTTP handles the client WebSocket upgrade at /ws.
func (ws *GameServerWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !ws.status.IsPrimary() {
		http.Error(w, "not primary for this match", http.StatusServiceUnavailable)
		return
	}

	playerID := r.URL.Query().Get("player")
	if playerID == "" {
		playerID = "guest-" + r.RemoteAddr
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		log.Printf("[WS] Accept error: %v", err)
		return
	}
	defer func() { _ = conn.CloseNow() }()

	ws.mu.Lock()
	ws.conns[conn] = playerID
	ws.mu.Unlock()

	defer func() {
		ws.mu.Lock()
		delete(ws.conns, conn)
		ws.mu.Unlock()
	}()

	// Send initial state snapshot on connect
	ws.sendCurrentState(conn)

	// Message ingest loop
	for {
		_, data, err := conn.Read(context.Background())
		if err != nil {
			break
		}

		if !ws.status.IsPrimary() {
			_ = conn.Close(websocket.StatusServiceRestart, "stepping down")
			break
		}

		// Support test client pings ("inc")
		textMsg := strings.TrimSpace(string(data))
		if textMsg == "inc" {
			n := ws.counter.Add(1)
			info := ws.status.Info()
			reply := fmt.Sprintf("%s|%s|%d", info.Pod, info.MatchID, n)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = conn.Write(ctx, websocket.MessageText, []byte(reply))
			cancel()
			continue
		}

		// Support structured Uno game actions
		var env protocol.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			continue
		}

		if env.Type == protocol.TypeActionRequest {
			var act uno.Action
			if err := json.Unmarshal(env.Payload, &act); err == nil {
				act.PlayerID = playerID
				game := ws.getGame()
				if game != nil {
					if _, err := game.ApplyAction(act); err != nil {
						ws.sendError(conn, "ILLEGAL_MOVE", err.Error())
					} else {
						ws.triggerBroadcast()
					}
				}
			}
		}
	}
}

func (ws *GameServerWS) sendCurrentState(conn *websocket.Conn) {
	game := ws.getGame()
	if game == nil {
		return
	}
	snap, err := game.ExportSnapshot()
	if err != nil {
		return
	}

	env := protocol.Envelope{
		Type:      protocol.TypeStateBroadcast,
		MatchID:   game.MatchID(),
		Sequence:  game.Sequence(),
		Timestamp: time.Now().UnixMilli(),
		Payload:   snap,
	}
	bytes, _ := json.Marshal(env)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, bytes)
}

func (ws *GameServerWS) triggerBroadcast() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for c := range ws.conns {
		go ws.sendCurrentState(c)
	}
}

func (ws *GameServerWS) sendError(conn *websocket.Conn, code, message string) {
	errPayload, _ := json.Marshal(protocol.ErrorPayload{
		Code:    code,
		Message: message,
	})
	env := protocol.Envelope{
		Type:      protocol.TypeErrorResponse,
		Timestamp: time.Now().UnixMilli(),
		Payload:   errPayload,
	}
	data, err := json.Marshal(env)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, data)
}

func (ws *GameServerWS) CloseAllDemoted() {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for c := range ws.conns {
		go func(conn *websocket.Conn) {
			_ = conn.Close(websocket.StatusServiceRestart, "pod demoted")
		}(c)
	}
}
