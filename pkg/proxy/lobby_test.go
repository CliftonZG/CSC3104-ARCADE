package proxy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProxyLobbyAggregationAndMatchmake(t *testing.T) {
	// Upstream pod A serves room-a with 1 occupied seat (human waiting).
	podA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/rooms":
			writeJSON(w, http.StatusOK, map[string]any{
				"rooms": []RoomSummary{
					{
						MatchID:       "room-a",
						Status:        "in_progress",
						Sequence:      3,
						Players:       []string{"player1", "player2", "player3", "player4"},
						OccupiedSeats: []string{"player1"},
						OpenSeats:     []string{"player2", "player3", "player4"},
					},
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/rooms":
			writeJSON(w, http.StatusCreated, RoomSummary{
				MatchID:   "room-new",
				Status:    "in_progress",
				Players:   []string{"player1", "player2", "player3", "player4"},
				OpenSeats: []string{"player1", "player2", "player3", "player4"},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer podA.Close()

	// Upstream pod B serves room-b with 0 occupied seats.
	podB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/api/rooms" {
			writeJSON(w, http.StatusOK, map[string]any{
				"rooms": []RoomSummary{
					{
						MatchID:   "room-b",
						Status:    "in_progress",
						Sequence:  0,
						Players:   []string{"player1", "player2", "player3", "player4"},
						OpenSeats: []string{"player1", "player2", "player3", "player4"},
					},
				},
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer podB.Close()

	p := New(Config{})
	addrA := strings.TrimPrefix(podA.URL, "http://")
	addrB := strings.TrimPrefix(podB.URL, "http://")
	_, _ = p.Table().Apply(Update{MatchID: "room-a", Epoch: 1, PrimaryAddr: addrA})
	_, _ = p.Table().Apply(Update{MatchID: "room-b", Epoch: 1, PrimaryAddr: addrB})

	// 1. GET /api/rooms aggregates both upstreams.
	req := httptest.NewRequest(http.MethodGet, "/api/rooms", nil)
	rec := httptest.NewRecorder()
	p.HandleRooms(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/rooms status = %d, want 200", rec.Code)
	}
	var env roomsEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(env.Rooms) != 2 {
		t.Fatalf("expected 2 aggregated rooms, got %d", len(env.Rooms))
	}

	// 2. POST /api/matchmake prefers room-a because it has a human player waiting.
	req = httptest.NewRequest(http.MethodPost, "/api/matchmake", bytes.NewBufferString(`{}`))
	rec = httptest.NewRecorder()
	p.HandleMatchmake(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("POST /api/matchmake status = %d, want 200", rec.Code)
	}
	var mm struct {
		MatchID  string `json:"match_id"`
		PlayerID string `json:"player_id"`
		Created  bool   `json:"created"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &mm); err != nil {
		t.Fatalf("unmarshal matchmake: %v", err)
	}
	if mm.MatchID != "room-a" || mm.PlayerID != "player2" || mm.Created {
		t.Fatalf("unexpected matchmake result: %+v", mm)
	}
}
