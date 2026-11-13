package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sort"
	"strings"

	"github.com/CSC3104-ARCADE/pkg/uno"
)

// RoomInfo summarizes a live match session and its seat occupancy for the lobby.
type RoomInfo struct {
	MatchID           string   `json:"match_id"`
	Status            string   `json:"status"`
	Sequence          int64    `json:"sequence"`
	Winner            string   `json:"winner,omitempty"`
	Players           []string `json:"players"`
	OccupiedSeats     []string `json:"occupied_seats"`
	OpenSeats         []string `json:"open_seats"`
	ActiveConnections int      `json:"active_connections"`
}

type createRoomRequest struct {
	MatchID string `json:"match_id"`
}

type matchmakeResponse struct {
	MatchID  string `json:"match_id"`
	PlayerID string `json:"player_id"`
	Created  bool   `json:"created"`
}

// HandleRooms serves GET /api/rooms (list active rooms) and POST /api/rooms (create or ensure a room).
func (ws *GameServerWS) HandleRooms(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeLobbyJSON(w, http.StatusOK, map[string]any{"rooms": ws.listRooms()})
	case http.MethodPost:
		if ws.status != nil && !ws.status.IsPrimary() {
			writeLobbyJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "pod is not primary"})
			return
		}
		var req createRoomRequest
		if r.Body != nil {
			_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&req)
		}
		matchID := strings.TrimSpace(req.MatchID)
		if matchID == "" {
			matchID = generateRoomID()
		}
		if !isValidRoomID(matchID) {
			writeLobbyJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid match_id (use 1-64 chars: A-Z, a-z, 0-9, _, -)"})
			return
		}
		ws.getOrCreateSession(matchID)
		for _, room := range ws.listRooms() {
			if room.MatchID == matchID {
				writeLobbyJSON(w, http.StatusCreated, room)
				return
			}
		}
		writeLobbyJSON(w, http.StatusInternalServerError, map[string]string{"error": "failed to initialize room"})
	default:
		w.Header().Set("Allow", "GET, POST")
		writeLobbyJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// HandleMatchmake serves POST /api/matchmake, placing the caller into an open seat
// on an active match (preferring rooms with human players already waiting) or creating a new room.
func (ws *GameServerWS) HandleMatchmake(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeLobbyJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if ws.status != nil && !ws.status.IsPrimary() {
		writeLobbyJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "pod is not primary"})
		return
	}

	rooms := ws.listRooms()
	var fallback *RoomInfo
	for i := range rooms {
		room := &rooms[i]
		if room.Status != string(uno.StatusInProgress) || len(room.OpenSeats) == 0 {
			continue
		}
		if len(room.OccupiedSeats) > 0 {
			writeLobbyJSON(w, http.StatusOK, matchmakeResponse{
				MatchID:  room.MatchID,
				PlayerID: room.OpenSeats[0],
				Created:  false,
			})
			return
		}
		if fallback == nil {
			fallback = room
		}
	}

	if fallback != nil {
		writeLobbyJSON(w, http.StatusOK, matchmakeResponse{
			MatchID:  fallback.MatchID,
			PlayerID: fallback.OpenSeats[0],
			Created:  false,
		})
		return
	}

	matchID := generateRoomID()
	ws.getOrCreateSession(matchID)
	writeLobbyJSON(w, http.StatusOK, matchmakeResponse{
		MatchID:  matchID,
		PlayerID: "player1",
		Created:  true,
	})
}

func (ws *GameServerWS) listRooms() []RoomInfo {
	connectedByMatch := make(map[string]map[string]bool)
	connCountByMatch := make(map[string]int)

	ws.mu.Lock()
	for _, client := range ws.conns {
		connCountByMatch[client.matchID]++
		if connectedByMatch[client.matchID] == nil {
			connectedByMatch[client.matchID] = make(map[string]bool)
		}
		connectedByMatch[client.matchID][client.playerID] = true
	}
	ws.mu.Unlock()

	ws.sessionsMu.RLock()
	sessions := make(map[string]EngineSession, len(ws.sessions))
	for k, v := range ws.sessions {
		sessions[k] = v
	}
	ws.sessionsMu.RUnlock()

	rooms := make([]RoomInfo, 0, len(sessions))
	for matchID, sess := range sessions {
		st := sess.State()
		if st == nil {
			continue
		}
		occupied := make([]string, 0, len(st.Players))
		open := make([]string, 0, len(st.Players))
		connSeats := connectedByMatch[matchID]
		for _, p := range st.Players {
			if connSeats[p] {
				occupied = append(occupied, p)
			} else {
				open = append(open, p)
			}
		}
		playersCopy := append([]string(nil), st.Players...)
		rooms = append(rooms, RoomInfo{
			MatchID:           matchID,
			Status:            string(st.Status),
			Sequence:          st.Sequence,
			Winner:            st.Winner,
			Players:           playersCopy,
			OccupiedSeats:     occupied,
			OpenSeats:         open,
			ActiveConnections: connCountByMatch[matchID],
		})
	}

	sort.Slice(rooms, func(i, j int) bool {
		return rooms[i].MatchID < rooms[j].MatchID
	})
	return rooms
}

func generateRoomID() string {
	var b [2]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "room-1000"
	}
	return "room-" + hex.EncodeToString(b[:])
}

func isValidRoomID(id string) bool {
	if len(id) == 0 || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

func writeLobbyJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
