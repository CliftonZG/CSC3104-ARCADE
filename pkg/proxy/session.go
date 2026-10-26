package proxy

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

var (
	errClientGone = errors.New("client connection ended")
	errFenced     = errors.New("upstream was fenced off")
	errRerouted   = errors.New("route changed while dialing")
)

// rejectedError is returned by dial when the game pod answered the WebSocket
// handshake with an HTTP status other than 101. coder/websocket reports that
// as a plain error plus the response, so the status is kept here.
type rejectedError struct {
	status int
	err    error
}

func (e *rejectedError) Error() string { return e.err.Error() }
func (e *rejectedError) Unwrap() error { return e.err }

// A session is one client WebSocket and everything the proxy does on its
// behalf. The client socket is held for the whole life of the session; the
// upstream socket to the game pod is replaced whenever the primary changes.
//
//	client <──── (stays open) ────> proxy <──> upstream A   (revoked, fenced)
//	                                      └──> upstream B   (new primary)
type session struct {
	p       *Proxy
	cfg     *Config
	log     *slog.Logger
	matchID string
	query   string // raw query string, forwarded to the game pod unchanged
	remote  string // client IP, for X-Forwarded-For
	client  *websocket.Conn

	ctx    context.Context
	cancel context.CancelCauseFunc

	// sendMu serialises everything written towards the game pod: client
	// messages, and the replay of buffered messages onto a new upstream. It
	// guards up and the pending queue.
	sendMu       sync.Mutex
	up           *upstream
	pending      []message
	pendingBytes int

	// curMu is the fence. Only the upstream stored in cur may deliver to the
	// client, and delivering happens under the read lock. Clearing cur takes
	// the write lock, so once it returns no message from the old primary is
	// in flight to the client or can ever reach it.
	curMu sync.RWMutex
	cur   *upstream

	endMu       sync.Mutex
	closeCode   websocket.StatusCode // sent to the client when the session ends; 0 = just drop
	closeReason string

	// clientClose records how the client left: its close status, or -1 for an
	// abrupt disconnect. 0 means it has not left.
	clientClose atomic.Int32
}

type message struct {
	typ  websocket.MessageType
	data []byte
}

// upstream is one connection to one game pod, bound to one route (epoch).
type upstream struct {
	conn  *websocket.Conn
	route Route
	born  time.Time
	done  chan struct{} // closed when the read pump exits
	err   error         // why it exited; valid once done is closed
}

func newSession(p *Proxy, client *websocket.Conn, matchID, query, remote string) *session {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &session{
		p:       p,
		cfg:     &p.cfg,
		log:     p.log.With("match", matchID, "client", remote),
		matchID: matchID,
		query:   query,
		remote:  remote,
		client:  client,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// end records how to close the client and stops the session. The first call
// wins; later calls (for example the cancellation that follows) change
// nothing.
func (s *session) end(code websocket.StatusCode, reason string) {
	s.endMu.Lock()
	if s.closeCode == 0 {
		s.closeCode, s.closeReason = code, reason
	}
	s.endMu.Unlock()
	s.cancel(fmt.Errorf("session ended: %d %s", int(code), reason))
}

func (s *session) closeInfo() (websocket.StatusCode, string) {
	s.endMu.Lock()
	defer s.endMu.Unlock()
	return s.closeCode, s.closeReason
}

// run drives the session to completion. It returns once the client socket is
// closed and every helper goroutine has exited.
func (s *session) run() {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.readClient() }()
	if s.cfg.ClientPingInterval > 0 {
		wg.Add(1)
		go func() { defer wg.Done(); s.pingClient() }()
	}

	s.loop()
	s.finish()
	wg.Wait()
}

// ---------------------------------------------------------------------------
// the connect / retarget loop
// ---------------------------------------------------------------------------

// maxQuickFailures bounds how many times in a row an upstream may accept a
// connection and then drop it within a second before the proxy gives up.
const maxQuickFailures = 8

func (s *session) loop() {
	w := s.p.table.Watch(s.matchID) // before the first Lookup: no update can slip through
	defer w.Stop()

	outageStart := time.Now()
	attempt := 0
	quick := 0
	var prev *Route

	for {
		if s.ctx.Err() != nil {
			return
		}
		if time.Since(outageStart) >= s.cfg.MaxOutage {
			s.p.stats.OutageTimeouts.Add(1)
			s.log.Warn("no game server became available in time", "max_outage", s.cfg.MaxOutage)
			s.end(websocket.StatusTryAgainLater, "no game server available")
			return
		}

		route, ok := s.p.table.Lookup(s.matchID)
		if !ok {
			s.p.hint(s.matchID)
			s.wait(w, outageStart, 0) // until a route shows up or the outage cap
			continue
		}

		up, err := s.dial(route, w)
		if err != nil {
			if errors.Is(err, errRerouted) {
				continue // aimed at a revoked primary; go straight to the new one
			}
			if s.ctx.Err() != nil {
				return
			}
			var rej *rejectedError
			if errors.As(err, &rej) && (rej.status == http.StatusBadRequest ||
				rej.status == http.StatusUnauthorized || rej.status == http.StatusForbidden) {
				s.log.Warn("game server rejected the client", "status", rej.status, "addr", route.Addr)
				s.end(websocket.StatusPolicyViolation, fmt.Sprintf("rejected by game server (HTTP %d)", rej.status))
				return
			}
			s.p.stats.DialFailures.Add(1)
			s.log.Info("cannot reach primary", "addr", route.Addr, "epoch", route.Epoch, "err", err)
			s.p.hint(s.matchID)
			s.wait(w, outageStart, s.backoff(attempt))
			attempt++
			continue
		}

		if err := s.install(up); err != nil {
			_ = up.conn.CloseNow()
			s.log.Info("could not replay buffered messages to primary", "addr", route.Addr, "err", err)
			s.wait(w, outageStart, s.backoff(attempt))
			attempt++
			continue
		}
		go s.pump(up)
		if s.cfg.UpstreamPingInterval > 0 {
			go s.pingUpstream(up)
		}
		if prev != nil && *prev != route {
			s.p.stats.Retargets.Add(1)
			s.log.Info("client moved to new primary", "from", prev.Addr, "to", route.Addr, "epoch", route.Epoch)
		}
		r := route
		prev = &r

		switch res := s.serve(up, w); {
		case res.done:
			s.fence(up)
			// Tell the game pod why the player's socket went away: a clean
			// goodbye from the client is 1000; anything else (abrupt loss,
			// proxy shutdown, proxy-side error) is 1001.
			closeCode := websocket.StatusGoingAway
			if s.clientCloseCode() == websocket.StatusNormalClosure {
				closeCode = websocket.StatusNormalClosure
			}
			go func() { _ = up.conn.Close(closeCode, "client disconnected") }()
			return

		case res.rerouted:
			s.revoke(up)
			attempt, quick = 0, 0
			outageStart = time.Now()

		default: // the upstream ended by itself
			s.revoke(up)
			if term, code, reason := classifyUpstreamEnd(res.err); term {
				s.log.Info("game server ended the session", "code", int(code), "reason", reason)
				s.end(code, reason)
				return
			}
			s.log.Info("lost primary", "addr", route.Addr, "epoch", route.Epoch, "err", res.err)
			s.p.hint(s.matchID)
			outageStart = time.Now()
			if time.Since(up.born) < time.Second {
				attempt++
				quick++
				if quick >= maxQuickFailures {
					s.end(websocket.StatusBadGateway, "game server keeps closing the connection")
					return
				}
				s.wait(w, outageStart, s.backoff(attempt-1))
			} else {
				attempt, quick = 0, 0
			}
		}
	}
}

type serveResult struct {
	done     bool // the session is ending
	rerouted bool // the control plane moved the match elsewhere
	err      error
}

// serve blocks while up is the live primary. It returns when the session
// ends, the route changes, or the upstream drops.
func (s *session) serve(up *upstream, w *Watcher) serveResult {
	for {
		// Checked before every wait, so a change whose signal was consumed
		// elsewhere (for instance by an aborted dial) is still noticed.
		if r, ok := s.p.table.Lookup(s.matchID); !ok || r != up.route {
			return serveResult{rerouted: true}
		}
		select {
		case <-s.ctx.Done():
			return serveResult{done: true}
		case <-up.done:
			if s.ctx.Err() != nil {
				return serveResult{done: true}
			}
			return serveResult{err: up.err}
		case <-w.C():
		}
	}
}

// wait sleeps for d (or, if d <= 0, until the outage cap), but wakes early
// for a route change or when the session ends.
func (s *session) wait(w *Watcher, outageStart time.Time, d time.Duration) {
	remaining := s.cfg.MaxOutage - time.Since(outageStart)
	if remaining <= 0 {
		return
	}
	if d <= 0 || d > remaining {
		d = remaining
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-w.C():
	case <-s.ctx.Done():
	}
}

// backoff returns an exponential, jittered delay for the n-th failed attempt.
func (s *session) backoff(n int) time.Duration {
	if n > 20 {
		n = 20
	}
	d := s.cfg.BackoffMin << uint(n)
	if d > s.cfg.BackoffMax || d <= 0 {
		d = s.cfg.BackoffMax
	}
	half := d / 2
	return half + time.Duration(rand.Int63n(int64(half)+1))
}

// dial connects to the primary named by route. If the route changes while the
// dial is in progress the dial is abandoned and errRerouted is returned.
func (s *session) dial(route Route, w *Watcher) (*upstream, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.cfg.DialTimeout)
	defer cancel()

	var aborted atomic.Bool
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-w.C():
				// The signal may be old news (the update that produced the
				// route we are dialing). Only abort if the route differs.
				if r, ok := s.p.table.Lookup(s.matchID); !ok || r != route {
					aborted.Store(true)
					cancel()
					return
				}
			case <-ctx.Done():
				return
			case <-stop:
				return
			}
		}
	}()

	hdr := http.Header{}
	hdr.Set("X-Arcade-Match", s.matchID)
	hdr.Set("X-Arcade-Epoch", strconv.FormatUint(route.Epoch, 10))
	hdr.Set("X-Forwarded-For", s.remote)
	target := "ws://" + route.Addr + s.cfg.UpstreamPath
	if s.query != "" {
		target += "?" + s.query
	}

	conn, resp, err := websocket.Dial(ctx, target, &websocket.DialOptions{
		HTTPClient:      s.p.httpClient,
		HTTPHeader:      hdr,
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		if aborted.Load() {
			return nil, errRerouted
		}
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return nil, &rejectedError{status: resp.StatusCode, err: err}
		}
		return nil, err
	}
	conn.SetReadLimit(s.cfg.UpstreamReadLimit)
	return &upstream{conn: conn, route: route, born: time.Now(), done: make(chan struct{})}, nil
}

// install makes up the live upstream. Messages the client sent while the
// match had no primary are replayed first, in their original order, and only
// then does the client's traffic resume, so ordering is preserved.
func (s *session) install(up *upstream) error {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	for len(s.pending) > 0 {
		if err := s.writeUp(up.conn, s.pending[0]); err != nil {
			return err // the head stays queued; it may be sent again later
		}
		s.pendingBytes -= len(s.pending[0].data)
		s.pending[0] = message{}
		s.pending = s.pending[1:]
	}
	s.pending, s.pendingBytes = nil, 0

	s.curMu.Lock()
	s.cur = up
	s.curMu.Unlock()
	s.up = up
	return nil
}

// fence stops up from delivering anything more to the client. When it
// returns, no message from up is in flight to the client and none will be.
func (s *session) fence(up *upstream) {
	s.curMu.Lock()
	if s.cur == up {
		s.cur = nil
	}
	s.curMu.Unlock()
}

// revoke fences up and then drops its socket without a closing handshake.
//
// The order matters. Closing the socket is what releases a forward() that is
// stuck writing to an unresponsive primary while holding sendMu. forward then
// queues its message, and install replays it to the next primary. (s.up keeps
// pointing at the dead upstream until then; writing to it just fails.)
func (s *session) revoke(up *upstream) {
	s.fence(up)
	_ = up.conn.CloseNow()
}

// ---------------------------------------------------------------------------
// client -> upstream
// ---------------------------------------------------------------------------

func (s *session) readClient() {
	for {
		typ, data, err := s.client.Read(context.Background())
		if err != nil {
			if code := websocket.CloseStatus(err); code != -1 {
				s.clientClose.CompareAndSwap(0, int32(code))
			} else {
				s.clientClose.CompareAndSwap(0, -1)
			}
			s.cancel(errClientGone)
			return
		}
		s.forward(typ, data)
	}
}

func (s *session) clientCloseCode() websocket.StatusCode {
	return websocket.StatusCode(s.clientClose.Load())
}

func (s *session) writeUp(c *websocket.Conn, m message) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.WriteTimeout)
	defer cancel()
	return c.Write(ctx, m.typ, m.data)
}

// forward sends a client message to the current primary, or queues it if the
// match has none right now.
func (s *session) forward(typ websocket.MessageType, data []byte) {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	m := message{typ: typ, data: data}
	if up := s.up; up != nil {
		if err := s.writeUp(up.conn, m); err == nil {
			s.p.stats.ClientMessages.Add(1)
			return
		}
		// The primary is unreachable or stalled. Keep the message (it will be
		// replayed to the next primary) and drop this upstream so the loop
		// notices.
		s.up = nil
		_ = up.conn.CloseNow()
	}

	if len(s.pending)+1 > s.cfg.PendingMaxMessages || s.pendingBytes+len(data) > s.cfg.PendingMaxBytes {
		s.p.stats.BufferOverflows.Add(1)
		s.end(websocket.StatusTryAgainLater, "too many messages while the game server is unavailable")
		return
	}
	s.pending = append(s.pending, m)
	s.pendingBytes += len(data)
	s.p.stats.BufferedMsgs.Add(1)
}

// ---------------------------------------------------------------------------
// upstream -> client
// ---------------------------------------------------------------------------

func (s *session) pump(up *upstream) {
	defer close(up.done)
	for {
		typ, data, err := up.conn.Read(context.Background())
		if err != nil {
			up.err = err
			return
		}
		if !s.deliver(up, typ, data) {
			if s.ctx.Err() != nil {
				up.err = errClientGone
			} else {
				up.err = errFenced
			}
			return
		}
	}
}

// deliver hands one upstream message to the client, unless up has been
// revoked. It reports whether the pump should keep going.
func (s *session) deliver(up *upstream, typ websocket.MessageType, data []byte) bool {
	s.curMu.RLock()
	defer s.curMu.RUnlock()

	if s.cur != up {
		s.p.stats.FencedDrops.Add(1)
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.cfg.WriteTimeout)
	defer cancel()
	if err := s.client.Write(ctx, typ, data); err != nil {
		s.clientClose.CompareAndSwap(0, -1)
		s.cancel(errClientGone)
		_ = s.client.CloseNow()
		return false
	}
	s.p.stats.UpstreamMessage.Add(1)
	return true
}

// classifyUpstreamEnd decides whether the game pod closing its socket means
// "this player's session is over" (terminal: pass the close on to the client)
// or "this pod is going away" (not terminal: find the next primary).
func classifyUpstreamEnd(err error) (terminal bool, code websocket.StatusCode, reason string) {
	var ce websocket.CloseError
	if !errors.As(err, &ce) {
		return false, 0, "" // EOF, reset, timeout: the pod died
	}
	switch c := ce.Code; {
	case c == websocket.StatusNormalClosure, c == websocket.StatusPolicyViolation, c == websocket.StatusMessageTooBig,
		c >= 3000 && c <= 4999:
		return true, c, ce.Reason
	case c == websocket.StatusProtocolError, c == websocket.StatusUnsupportedData,
		c == websocket.StatusInvalidFramePayloadData, c == websocket.StatusMandatoryExtension:
		return true, websocket.StatusInternalError, "game server protocol error"
	default:
		// 1001 going away, 1011, 1012 service restart, 1013 try again later,
		// 1014, 1005 and anything unknown: the pod is stepping down.
		return false, 0, ""
	}
}

// ---------------------------------------------------------------------------
// keep-alives
// ---------------------------------------------------------------------------

func (s *session) pingClient() {
	t := time.NewTicker(s.cfg.ClientPingInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PingTimeout)
		err := s.client.Ping(ctx)
		cancel()
		if err != nil && s.ctx.Err() == nil {
			s.log.Info("client did not answer ping", "err", err)
			s.clientClose.CompareAndSwap(0, -1)
			s.cancel(errClientGone)
			_ = s.client.CloseNow()
			return
		}
	}
}

func (s *session) pingUpstream(up *upstream) {
	t := time.NewTicker(s.cfg.UpstreamPingInterval)
	defer t.Stop()
	for {
		select {
		case <-up.done:
			return
		case <-s.ctx.Done():
			return
		case <-t.C:
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.cfg.PingTimeout)
		err := up.conn.Ping(ctx)
		cancel()
		if err != nil {
			s.log.Info("primary did not answer ping", "addr", up.route.Addr, "err", err)
			_ = up.conn.CloseNow() // the pump sees the error and the loop moves on
			return
		}
	}
}

// ---------------------------------------------------------------------------
// teardown
// ---------------------------------------------------------------------------

func (s *session) finish() {
	s.cancel(nil) // no-op if something already ended the session

	code, reason := s.closeInfo()
	if code != 0 {
		_ = s.client.Close(code, reason)
	}
	_ = s.client.CloseNow()
}
