package proxy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProxyClusterAccessControlAndProbe(t *testing.T) {
	pod := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/role" {
			writeJSON(w, http.StatusOK, map[string]any{
				"pod":      "arcade-gs-a-0",
				"match_id": "room0",
				"role":     "primary",
				"epoch":    2,
				"ready":    true,
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer pod.Close()

	addr := strings.TrimPrefix(pod.URL, "http://")
	p := New(Config{AdminToken: "ops-secret"})
	_, _ = p.Table().Apply(Update{MatchID: "room0", Epoch: 2, PrimaryAddr: addr})

	// 1. Missing token -> 401 Unauthorized (even on loopback)
	req := httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	req.RemoteAddr = "127.0.0.1:4444"
	rec := httptest.NewRecorder()
	p.HandleCluster(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token status = %d, want 401", rec.Code)
	}

	// 2. Valid bearer token -> 200 OK with pod & route details
	req = httptest.NewRequest(http.MethodGet, "/api/cluster", nil)
	req.RemoteAddr = "203.0.113.20:4444"
	req.Header.Set("Authorization", "Bearer ops-secret")
	rec = httptest.NewRecorder()
	p.HandleCluster(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token status = %d, want 200", rec.Code)
	}
	var resp struct {
		Mode   string         `json:"mode"`
		Pods   []clusterPod   `json:"pods"`
		Routes []clusterRoute `json:"routes"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if resp.Mode != "proxy" || len(resp.Pods) != 1 || !resp.Pods[0].Reachable || resp.Pods[0].Role != "primary" {
		t.Fatalf("unexpected pods: %+v", resp.Pods)
	}
	if len(resp.Routes) != 1 || resp.Routes[0].MatchID != "room0" || resp.Routes[0].Epoch != 2 {
		t.Fatalf("unexpected routes: %+v", resp.Routes)
	}
}
