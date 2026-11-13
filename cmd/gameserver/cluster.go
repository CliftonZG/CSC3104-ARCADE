package main

import (
	"crypto/subtle"
	"net"
	"net/http"
	"os"
	"strings"
)

type clusterStats struct {
	ActiveSessions   int64 `json:"active_sessions"`
	Retargets        int64 `json:"retargets"`
	BufferedMessages int64 `json:"buffered_messages"`
	DialFailures     int64 `json:"dial_failures"`
}

type clusterRoute struct {
	MatchID     string `json:"match_id"`
	Epoch       int64  `json:"epoch"`
	PrimaryAddr string `json:"primary_addr"`
}

type clusterPod struct {
	Addr      string `json:"addr"`
	Reachable bool   `json:"reachable"`
	Pod       string `json:"pod"`
	MatchID   string `json:"match_id"`
	Role      string `json:"role"`
	Epoch     int64  `json:"epoch"`
	Ready     bool   `json:"ready"`
	Peer      string `json:"peer,omitempty"`
}

// HandleCluster serves GET /api/cluster with live gameserver role, epoch, and active room routes.
// Access is restricted to callers presenting ARCADE_ADMIN_TOKEN (if set) or direct loopback clients.
func (ws *GameServerWS) HandleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeLobbyJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if !authorizeLocalOrToken(w, r, os.Getenv("ARCADE_ADMIN_TOKEN")) {
		return
	}

	ws.mu.Lock()
	activeConns := int64(len(ws.conns))
	ws.mu.Unlock()

	var pods []clusterPod
	var podEpoch int64
	if ws.status != nil {
		info := ws.status.Info()
		podEpoch = info.Epoch
		podName := info.Pod
		if podName == "" {
			podName = "local-gameserver"
		}
		pods = append(pods, clusterPod{
			Addr:      "local",
			Reachable: true,
			Pod:       podName,
			MatchID:   info.MatchID,
			Role:      string(info.Role),
			Epoch:     info.Epoch,
			Ready:     info.Ready,
			Peer:      info.Peer,
		})
	}

	rooms := ws.listRooms()
	routes := make([]clusterRoute, 0, len(rooms))
	for _, rm := range rooms {
		routes = append(routes, clusterRoute{
			MatchID:     rm.MatchID,
			Epoch:       podEpoch,
			PrimaryAddr: "local",
		})
	}

	writeLobbyJSON(w, http.StatusOK, map[string]any{
		"mode":   "gameserver",
		"stats":  clusterStats{ActiveSessions: activeConns},
		"routes": routes,
		"pods":   pods,
	})
}

func authorizeLocalOrToken(w http.ResponseWriter, r *http.Request, token string) bool {
	if token != "" {
		authz := r.Header.Get("Authorization")
		got := strings.TrimPrefix(authz, "Bearer ")
		if !strings.HasPrefix(authz, "Bearer ") || subtle.ConstantTimeCompare([]byte(got), []byte(token)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="arcade-cluster"`)
			writeLobbyJSON(w, http.StatusUnauthorized, map[string]string{"error": "operator token required"})
			return false
		}
		return true
	}

	if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("Forwarded") != "" || !isLoopbackAddr(r.RemoteAddr) {
		writeLobbyJSON(w, http.StatusForbidden, map[string]string{
			"error": "cluster inspection requires ARCADE_ADMIN_TOKEN outside localhost",
		})
		return false
	}
	return true
}

func isLoopbackAddr(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsLoopback()
}
