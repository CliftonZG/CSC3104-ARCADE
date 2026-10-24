// Package proxy is the data-plane ingress for ARCADE: it holds each player's
// WebSocket open and forwards it to whichever game pod is currently the
// primary for the player's match. When the control plane announces a new
// primary, the proxy moves every affected player to it without closing their
// sockets.
//
// The proxy decides nothing about who the primary is. It only follows the
// Table, and only ever to a higher epoch.
package proxy

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// MatchParam is the query parameter that carries the match id on the client's
// WebSocket URL, e.g. ws://proxy/ws?match=abc123&player=alice. The whole query
// string is forwarded to the game pod unchanged.
const MatchParam = "match"

// Proxy is the ingress. Mount it as the handler for the client WebSocket
// endpoint (for example "/ws").
type Proxy struct {
	cfg   Config
	table *Table
	stats Stats
	log   *slog.Logger

	// httpClient dials the game pods. It ignores HTTP_PROXY/HTTPS_PROXY (pods
	// are reached directly) and never follows redirects.
	httpClient *http.Client

	mu       sync.Mutex
	sessions map[*session]struct{}
	pending  int // handshakes in progress, counted against MaxSessions
	draining bool
	wg       sync.WaitGroup
}

// New creates a proxy with its own empty route table.
func New(cfg Config) *Proxy {
	cfg.applyDefaults()

	tr := &http.Transport{}
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		tr = base.Clone()
	}
	tr.Proxy = nil

	return &Proxy{
		cfg:   cfg,
		table: NewTable(),
		log:   cfg.Logger,
		httpClient: &http.Client{
			Transport:     tr,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		sessions: make(map[*session]struct{}),
	}
}

// Table is the route table. The control-plane integration calls Apply on it.
func (p *Proxy) Table() *Table { return p.table }

// Stats returns the live counters.
func (p *Proxy) Stats() *Stats { return &p.stats }

// Sessions returns the number of connected clients.
func (p *Proxy) Sessions() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sessions)
}

// ServeHTTP accepts a client WebSocket and proxies it until it ends.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	matchID := r.URL.Query().Get(MatchParam)
	if !ValidMatchID(matchID) {
		http.Error(w, "missing or invalid '"+MatchParam+"' query parameter", http.StatusBadRequest)
		return
	}

	p.mu.Lock()
	if p.draining || (p.cfg.MaxSessions > 0 && len(p.sessions)+p.pending >= p.cfg.MaxSessions) {
		p.mu.Unlock()
		w.Header().Set("Retry-After", "2")
		http.Error(w, "proxy is full or shutting down", http.StatusServiceUnavailable)
		return
	}
	p.pending++
	p.mu.Unlock()

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:     p.cfg.OriginPatterns,
		InsecureSkipVerify: p.cfg.InsecureSkipOriginCheck,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		p.mu.Lock()
		p.pending--
		p.mu.Unlock()
		p.log.Debug("client handshake rejected", "err", err)
		return
	}
	conn.SetReadLimit(p.cfg.ClientReadLimit)

	remote, _, splitErr := net.SplitHostPort(r.RemoteAddr)
	if splitErr != nil {
		remote = r.RemoteAddr
	}
	s := newSession(p, conn, matchID, r.URL.RawQuery, remote)

	p.mu.Lock()
	p.pending--
	if p.draining {
		p.mu.Unlock()
		_ = conn.Close(websocket.StatusGoingAway, "proxy shutting down")
		return
	}
	p.sessions[s] = struct{}{}
	p.wg.Add(1)
	p.mu.Unlock()

	p.stats.ActiveSessions.Add(1)
	p.stats.TotalSessions.Add(1)
	s.log.Info("client connected")

	s.run()

	p.stats.ActiveSessions.Add(-1)
	p.mu.Lock()
	delete(p.sessions, s)
	p.mu.Unlock()
	p.wg.Done()
	s.log.Info("client disconnected")
}

// Shutdown stops accepting clients, closes every session with 1001 (going
// away) and waits for them to finish or for ctx to end.
//
// A proxy restart drops its clients' sockets, which is the one thing this
// design cannot hide, so run several proxy replicas behind the load balancer
// and drain one at a time.
func (p *Proxy) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	p.draining = true
	live := make([]*session, 0, len(p.sessions))
	for s := range p.sessions {
		live = append(live, s)
	}
	p.mu.Unlock()

	for _, s := range live {
		s.end(websocket.StatusGoingAway, "proxy shutting down")
	}

	done := make(chan struct{})
	go func() { p.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
