package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/uno"
)

func TestGameServerClusterAccessControl(t *testing.T) {
	game, err := uno.NewGameEngine("room0", []string{"player1", "player2", "player3", "player4"}, 42)
	if err != nil {
		t.Fatalf("NewGameEngine: %v", err)
	}
	status := fleet.NewStatus(fleet.Identity{PodName: "gs-a-0", MatchID: "room0"}, fleet.RolePrimary)
	ws := NewGameServerWS(status, NewGameSession(game))

	// 1. Non-loopback without token -> 403 Forbidden
	req := httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	req.RemoteAddr = "203.0.113.10:5555"
	rec := httptest.NewRecorder()
	ws.HandleCluster(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("non-loopback status = %d, want 403", rec.Code)
	}

	// 2. Loopback without token -> 200 OK
	req = httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	rec = httptest.NewRecorder()
	ws.HandleCluster(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("loopback status = %d, want 200", rec.Code)
	}
	var body struct {
		Mode string       `json:"mode"`
		Pods []clusterPod `json:"pods"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if body.Mode != "gameserver" || len(body.Pods) != 1 || body.Pods[0].Pod != "gs-a-0" {
		t.Fatalf("unexpected cluster body: %+v", body)
	}

	// 3. With ARCADE_ADMIN_TOKEN set -> 401 without bearer, 200 with bearer
	t.Setenv("ARCADE_ADMIN_TOKEN", "secret-ops-token")
	req = httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	rec = httptest.NewRecorder()
	ws.HandleCluster(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	req.RemoteAddr = "203.0.113.10:5555"
	req.Header.Set("Authorization", "Bearer secret-ops-token")
	rec = httptest.NewRecorder()
	ws.HandleCluster(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token status = %d, want 200", rec.Code)
	}
}
