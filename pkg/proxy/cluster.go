package proxy

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"time"
)

type clusterRoute struct {
	MatchID     string `json:"match_id"`
	Epoch       uint64 `json:"epoch"`
	PrimaryAddr string `json:"primary_addr"`
}

type clusterPod struct {
	Addr      string `json:"addr"`
	Reachable bool   `json:"reachable"`
	Pod       string `json:"pod,omitempty"`
	MatchID   string `json:"match_id,omitempty"`
	Role      string `json:"role,omitempty"`
	Epoch     int64  `json:"epoch,omitempty"`
	Ready     bool   `json:"ready"`
	Peer      string `json:"peer,omitempty"`
	Error     string `json:"error,omitempty"`
}

// HandleCluster serves GET /api/cluster with live proxy stats, match routes, and pod roles.
// If AdminToken is set, callers must send Authorization: Bearer <token>.
// Otherwise, access is restricted to direct loopback clients.
func (p *Proxy) HandleCluster(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	if !p.authorizeCluster(w, r) {
		return
	}

	snap := p.table.Snapshot()
	routes := make([]clusterRoute, 0, len(snap))
	for matchID, rt := range snap {
		routes = append(routes, clusterRoute{
			MatchID:     matchID,
			Epoch:       rt.Epoch,
			PrimaryAddr: rt.Addr,
		})
	}
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].MatchID < routes[j].MatchID
	})

	addrs := p.upstreamAddrs()
	pods := make([]clusterPod, 0, len(addrs))
	for _, addr := range addrs {
		pods = append(pods, p.probePodRole(r.Context(), addr))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"mode":             "proxy",
		"default_upstream": p.table.Default(),
		"stats":            p.stats.Snapshot(),
		"routes":           routes,
		"pods":             pods,
	})
}

func (p *Proxy) authorizeCluster(w http.ResponseWriter, r *http.Request) bool {
	if tok := p.cfg.AdminToken; tok != "" {
		authz := r.Header.Get("Authorization")
		got := strings.TrimPrefix(authz, "Bearer ")
		if !strings.HasPrefix(authz, "Bearer ") || subtle.ConstantTimeCompare([]byte(got), []byte(tok)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="arcade-cluster"`)
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "operator token required"})
			return false
		}
		return true
	}

	if r.Header.Get("X-Forwarded-For") != "" || r.Header.Get("Forwarded") != "" || !isLoopbackRemote(r.RemoteAddr) {
		writeJSON(w, http.StatusForbidden, map[string]string{
			"error": "cluster inspection requires -admin-token outside localhost",
		})
		return false
	}
	return true
}

func isLoopbackRemote(remoteAddr string) bool {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	ip := net.ParseIP(strings.TrimSpace(host))
	return ip != nil && ip.IsLoopback()
}

func (p *Proxy) probePodRole(ctx context.Context, addr string) clusterPod {
	reqCtx, cancel := context.WithTimeout(ctx, 1500*time.Millisecond)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/role", nil)
	if err != nil {
		return clusterPod{Addr: addr, Reachable: false, Error: err.Error()}
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return clusterPod{Addr: addr, Reachable: false, Error: err.Error()}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return clusterPod{Addr: addr, Reachable: false, Error: resp.Status}
	}

	var info struct {
		Pod     string `json:"pod"`
		MatchID string `json:"match_id"`
		Role    string `json:"role"`
		Epoch   int64  `json:"epoch"`
		Ready   bool   `json:"ready"`
		Peer    string `json:"peer"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64*1024)).Decode(&info); err != nil {
		return clusterPod{Addr: addr, Reachable: false, Error: err.Error()}
	}
	if info.Pod == "" {
		info.Pod = addr
	}
	return clusterPod{
		Addr:      addr,
		Reachable: true,
		Pod:       info.Pod,
		MatchID:   info.MatchID,
		Role:      info.Role,
		Epoch:     info.Epoch,
		Ready:     info.Ready,
		Peer:      info.Peer,
	}
}
