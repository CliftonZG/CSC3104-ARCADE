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

type wsClient struct {
	playerID string
	matchID  string
}

// GameServerWS manages client WebSocket connections and per-match sessions.
// In Kubernetes, a pod pair serves its configured default match; in standalone
// or local multi-room mode, additional match IDs requested via ?match=... are
// lazily created and isolated by room.
type GameServerWS struct {
	status         *fleet.Status
	defaultSession EngineSession
	sessionsMu     sync.RWMutex
	sessions       map[string]EngineSession
	mu             sync.Mutex
	conns          map[*websocket.Conn]wsClient
	counter        atomic.Int64 // Handles "inc" messages from test clients
}

func NewGameServerWS(status *fleet.Status, session EngineSession) *GameServerWS {
	ws := &GameServerWS{
		status:         status,
		defaultSession: session,
		sessions:       make(map[string]EngineSession),
		conns:          make(map[*websocket.Conn]wsClient),
	}
	if session != nil {
		mid := session.MatchID()
		ws.sessions[mid] = session
		// Decoupled Observer Hook: automatically broadcast state transitions on every committed move
		session.RegisterHook(func(prev, next *uno.State, summary uno.ActionSummary) error {
			ws.triggerBroadcast(mid)
			return nil
		})
	}
	return ws
}

func (ws *GameServerWS) getOrCreateSession(matchID string) EngineSession {
	if matchID == "" && ws.defaultSession != nil {
		matchID = ws.defaultSession.MatchID()
	}

	ws.sessionsMu.RLock()
	sess, ok := ws.sessions[matchID]
	ws.sessionsMu.RUnlock()
	if ok {
		return sess
	}

	ws.sessionsMu.Lock()
	defer ws.sessionsMu.Unlock()
	if sess, ok := ws.sessions[matchID]; ok {
		return sess
	}

	g, err := uno.NewGameEngine(matchID, []string{"player1", "player2", "player3", "player4"}, time.Now().UnixNano())
	if err != nil {
		return ws.defaultSession
	}
	sess = NewGameSession(g)
	sess.RegisterHook(func(prev, next *uno.State, summary uno.ActionSummary) error {
		ws.triggerBroadcast(matchID)
		return nil
	})
	ws.sessions[matchID] = sess
	log.Printf("[WS] Created new concurrent room session: match=%s", matchID)
	return sess
}

func (ws *GameServerWS) resetSession(matchID string) {
	sess := ws.getOrCreateSession(matchID)
	if sess == nil {
		return
	}
	fresh, err := uno.NewGameEngine(sess.MatchID(), []string{"player1", "player2", "player3", "player4"}, time.Now().UnixNano())
	if err != nil {
		return
	}
	sess.SwapGame(fresh)
	log.Printf("[WS] Started new game for match=%s", sess.MatchID())
	ws.triggerBroadcast(sess.MatchID())
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
	matchID := r.URL.Query().Get("match")
	if matchID == "" && ws.defaultSession != nil {
		matchID = ws.defaultSession.MatchID()
	}
	sess := ws.getOrCreateSession(matchID)
	if sess != nil {
		matchID = sess.MatchID()
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
	ws.conns[conn] = wsClient{playerID: playerID, matchID: matchID}
	ws.mu.Unlock()

	defer func() {
		ws.mu.Lock()
		delete(ws.conns, conn)
		ws.mu.Unlock()
	}()

	// Send initial state snapshot on connect
	ws.sendCurrentState(conn, matchID)

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

		switch env.Type {
		case protocol.TypeActionRequest:
			var act uno.Action
			if err := json.Unmarshal(env.Payload, &act); err == nil {
				act.PlayerID = playerID
				sess := ws.getOrCreateSession(matchID)
				if sess != nil {
					if _, err := sess.ApplyAction(act); err != nil {
						ws.sendError(conn, "ILLEGAL_MOVE", err.Error())
					}
					// State broadcast is automatically triggered via session's MoveHook
				}
			}
		case protocol.TypeBotStep:
			ws.stepBot(matchID, playerID)
		case protocol.TypeNewGame:
			ws.resetSession(matchID)
		}
	}
}

// stepBot executes one legal move for the current player in matchID,
// provided the current player is not the requesting human seat.
func (ws *GameServerWS) stepBot(matchID, requestingPlayer string) {
	sess := ws.getOrCreateSession(matchID)
	if sess == nil {
		return
	}
	st := sess.State()
	if st == nil || st.Status == uno.StatusFinished {
		return
	}
	cur := st.CurrentPlayer()
	if cur == "" || cur == requestingPlayer {
		return
	}
	for idx, c := range st.Hands[cur] {
		if uno.IsPlayable(st, cur, c) {
			var col uno.Colour
			if c.IsWild() {
				col = uno.ColourRed
			}
			_, _ = sess.ApplyAction(uno.Action{
				Type:           uno.ActionPlay,
				PlayerID:       cur,
				CardIndex:      idx,
				DeclaredColour: col,
			})
			return
		}
	}
	if _, err := sess.ApplyAction(uno.Action{Type: uno.ActionDraw, PlayerID: cur}); err != nil {
		_, _ = sess.ApplyAction(uno.Action{Type: uno.ActionPass, PlayerID: cur})
	}
}

func (ws *GameServerWS) sendCurrentState(conn *websocket.Conn, matchID string) {
	sess := ws.getOrCreateSession(matchID)
	if sess == nil {
		return
	}
	snap, err := sess.ExportSnapshot()
	if err != nil {
		return
	}

	env := protocol.Envelope{
		Type:      protocol.TypeStateBroadcast,
		MatchID:   sess.MatchID(),
		Sequence:  sess.Sequence(),
		Timestamp: time.Now().UnixMilli(),
		Payload:   snap,
	}
	bytes, _ := json.Marshal(env)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, bytes)
}

func (ws *GameServerWS) triggerBroadcast(matchID string) {
	ws.mu.Lock()
	defer ws.mu.Unlock()
	for c, client := range ws.conns {
		if client.matchID == matchID {
			go ws.sendCurrentState(c, matchID)
		}
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
