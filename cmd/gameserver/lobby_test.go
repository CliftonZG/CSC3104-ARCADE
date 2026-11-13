package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/uno"
)

func TestGameServerLobbyAndMatchmake(t *testing.T) {
	game, err := uno.NewGameEngine("room0", []string{"player1", "player2", "player3", "player4"}, 42)
	if err != nil {
		t.Fatalf("NewGameEngine: %v", err)
	}
	status := fleet.NewStatus(fleet.Identity{MatchID: "room0"}, fleet.RolePrimary)
	ws := NewGameServerWS(status, NewGameSession(game))

	// 1. GET /api/rooms should list room0 with 4 open seats.
	req := httptest.NewRequest(http.MethodGet, "/api/rooms", nil)
	rec := httptest.NewRecorder()
	ws.HandleRooms(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/rooms status = %d, want 200", rec.Code)
	}
	var listResp struct {
		Rooms []RoomInfo `json:"rooms"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &listResp); err != nil {
		t.Fatalf("unmarshal rooms: %v", err)
	}
	if len(listResp.Rooms) != 1 || listResp.Rooms[0].MatchID != "room0" || len(listResp.Rooms[0].OpenSeats) != 4 {
		t.Fatalf("unexpected rooms: %+v", listResp.Rooms)
	}

	// 2. POST /api/rooms with explicit match_id creates room-alpha.
	req = httptest.NewRequest(http.MethodPost, "/api/rooms", bytes.NewBufferString(`{"match_id":"room-alpha"}`))
	rec = httptest.NewRecorder()
	ws.HandleRooms(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("POST /api/rooms status = %d, want 201", rec.Code)
	}
	var created RoomInfo
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatalf("unmarshal created room: %v", err)
	}
	if created.MatchID != "room-alpha" || len(created.OpenSeats) != 4 {
		t.Fatalf("unexpected created room: %+v", created)
	}

	// 3. POST /api/matchmake joins an open seat in an existing in_progress room.
	req = httptest.NewRequest(http.MethodPost, "/api/matchmake", bytes.NewBufferString(`{}`))
	rec = httptest.NewRecorder()
	ws.HandleMatchmake(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/matchmake status = %d, want 200", rec.Code)
	}
	var mm matchmakeResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &mm); err != nil {
		t.Fatalf("unmarshal matchmake: %v", err)
	}
	if mm.MatchID == "" || mm.PlayerID != "player1" || mm.Created {
		t.Fatalf("unexpected matchmake response: %+v", mm)
	}
}
