package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// ---------------------------------------------------------------------------
// test rig
// ---------------------------------------------------------------------------

// checkGoroutines fails the test if goroutines are still running after it
// finishes. Call it first so its cleanup runs last.
func checkGoroutines(t *testing.T) {
	t.Helper()
	before := runtime.NumGoroutine()
	t.Cleanup(func() {
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			if runtime.NumGoroutine() <= before {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		buf := make([]byte, 1<<16)
		n := runtime.Stack(buf, true)
		t.Errorf("goroutine leak: %d before, %d after\n%s", before, runtime.NumGoroutine(), buf[:n])
	})
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type gameReq struct {
	Header http.Header
	Path   string
	Query  string
}

// fakeGame is a stand-in game pod: it echoes "<name>|<message>".
type fakeGame struct {
	name string
	ln   net.Listener
	srv  *http.Server

	mu         sync.Mutex
	conns      []*websocket.Conn
	reqs       []gameReq
	recv       []string
	closeCodes []websocket.StatusCode
	status     int // if set, every handshake gets this HTTP status
	failFirst  int // the first N handshakes get 503
	maxConns   int // handshakes beyond this many get 503 (0 = unlimited)
	mute       bool
	flap       bool // accept, then immediately close with 1012

	// what GET /role answers; role == "" means the pod has no /role endpoint
	role      string
	roleEpoch int64
	roleMatch string
	notReady  bool

	killOnce sync.Once
}

func newFakeGame(t *testing.T, name string) *fakeGame {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := &fakeGame{name: name, ln: ln}
	g.srv = &http.Server{Handler: http.HandlerFunc(g.handle)}
	go func() { _ = g.srv.Serve(ln) }()
	t.Cleanup(g.Kill)
	return g
}

func (g *fakeGame) Addr() string { return g.ln.Addr().String() }

// setRole makes the pod answer GET /role like a real game pod does.
func (g *fakeGame) setRole(role string, epoch int64, match string) {
	g.mu.Lock()
	g.role, g.roleEpoch, g.roleMatch = role, epoch, match
	g.mu.Unlock()
}

func (g *fakeGame) serveRole(w http.ResponseWriter) {
	g.mu.Lock()
	role, epoch, match, ready := g.role, g.roleEpoch, g.roleMatch, !g.notReady
	g.mu.Unlock()
	if role == "" {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"pod": g.name, "match_id": match, "role": role, "epoch": epoch, "ready": ready,
	})
}

func (g *fakeGame) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == RolePath {
		g.serveRole(w)
		return
	}
	g.mu.Lock()
	g.reqs = append(g.reqs, gameReq{Header: r.Header.Clone(), Path: r.URL.Path, Query: r.URL.RawQuery})
	n := len(g.reqs)
	status := g.status
	if status == 0 && (n <= g.failFirst || (g.maxConns > 0 && n > g.maxConns)) {
		status = http.StatusServiceUnavailable
	}
	g.mu.Unlock()
	if status != 0 {
		http.Error(w, "not primary", status)
		return
	}

	c, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	c.SetReadLimit(1 << 20)
	g.mu.Lock()
	g.conns = append(g.conns, c)
	mute, flap := g.mute, g.flap
	g.mu.Unlock()
	if flap {
		_ = c.Close(websocket.StatusServiceRestart, "flapping")
		return
	}
	if mute {
		return // hold the socket open, never read, never answer pings
	}

	for {
		typ, data, err := c.Read(context.Background())
		if err != nil {
			g.mu.Lock()
			g.closeCodes = append(g.closeCodes, websocket.CloseStatus(err))
			g.mu.Unlock()
			return
		}
		g.mu.Lock()
		g.recv = append(g.recv, string(data))
		g.mu.Unlock()
		if c.Write(context.Background(), typ, append([]byte(g.name+"|"), data...)) != nil {
			return
		}
	}
}

// Kill simulates kill -9: the listener and every connection vanish at once.
func (g *fakeGame) Kill() {
	g.killOnce.Do(func() {
		_ = g.ln.Close()
		_ = g.srv.Close()
		g.mu.Lock()
		conns := append([]*websocket.Conn(nil), g.conns...)
		g.mu.Unlock()
		for _, c := range conns {
			_ = c.CloseNow()
		}
	})
}

// CloseAll closes every connection with a proper close frame.
func (g *fakeGame) CloseAll(code websocket.StatusCode, reason string) {
	g.mu.Lock()
	conns := append([]*websocket.Conn(nil), g.conns...)
	g.mu.Unlock()
	for _, c := range conns {
		go func(c *websocket.Conn) { _ = c.Close(code, reason) }(c)
	}
}

// Send pushes an unsolicited message to every connection.
func (g *fakeGame) Send(msg string) {
	g.mu.Lock()
	conns := append([]*websocket.Conn(nil), g.conns...)
	g.mu.Unlock()
	for _, c := range conns {
		_ = c.Write(context.Background(), websocket.MessageText, []byte(msg))
	}
}

func (g *fakeGame) setStatus(s int) { g.mu.Lock(); g.status = s; g.mu.Unlock() }

func (g *fakeGame) numReqs() int { g.mu.Lock(); defer g.mu.Unlock(); return len(g.reqs) }
func (g *fakeGame) req(i int) gameReq {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.reqs[i]
}
func (g *fakeGame) codes() []websocket.StatusCode {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]websocket.StatusCode(nil), g.closeCodes...)
}

type rig struct {
	p  *Proxy
	ts *httptest.Server
}

func newRig(t *testing.T, mod func(*Config)) *rig {
	t.Helper()
	cfg := Config{
		DialTimeout:          500 * time.Millisecond,
		BackoffMin:           5 * time.Millisecond,
		BackoffMax:           40 * time.Millisecond,
		MaxOutage:            5 * time.Second,
		ClientPingInterval:   -1,
		UpstreamPingInterval: -1,
	}
	if mod != nil {
		mod(&cfg)
	}
	p := New(cfg)
	ts := httptest.NewServer(p)
	t.Cleanup(ts.Close)
	return &rig{p: p, ts: ts}
}

func (r *rig) route(t *testing.T, match string, epoch uint64, g *fakeGame) {
	t.Helper()
	res, err := r.p.ApplyUpdate(Update{MatchID: match, Epoch: epoch, PrimaryAddr: g.Addr()})
	if err != nil || res != Applied {
		t.Fatalf("route %s epoch %d -> %s: result %q, err %v", match, epoch, g.name, res, err)
	}
}

func (r *rig) url(match string) string {
	return "ws" + strings.TrimPrefix(r.ts.URL, "http") + "/ws?match=" + match + "&player=alice"
}

// client wraps a connection with a background reader so tests can assert both
// what arrives and that nothing else does (no close, no error).
type client struct {
	c    *websocket.Conn
	msgs chan string
	errc chan error
}

func (r *rig) connect(t *testing.T, match string) *client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, r.url(match), nil)
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	c.SetReadLimit(1 << 20)
	cl := &client{c: c, msgs: make(chan string, 8192), errc: make(chan error, 1)}
	go func() {
		for {
			_, d, err := c.Read(context.Background())
			if err != nil {
				cl.errc <- err
				return
			}
			cl.msgs <- string(d)
		}
	}()
	t.Cleanup(func() { _ = c.CloseNow() })
	return cl
}

func (cl *client) send(t *testing.T, s string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.c.Write(ctx, websocket.MessageText, []byte(s)); err != nil {
		t.Fatalf("client send %q: %v", s, err)
	}
}

func (cl *client) next(t *testing.T) string {
	t.Helper()
	select {
	case m := <-cl.msgs:
		return m
	case err := <-cl.errc:
		t.Fatalf("client connection ended while waiting for a message: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
	}
	return ""
}

func (cl *client) expect(t *testing.T, want string) {
	t.Helper()
	if got := cl.next(t); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// expectQuiet asserts that for d no message arrives and the connection stays up.
func (cl *client) expectQuiet(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case m := <-cl.msgs:
		t.Fatalf("unexpected message %q", m)
	case err := <-cl.errc:
		t.Fatalf("connection ended unexpectedly: %v", err)
	case <-time.After(d):
	}
}

func (cl *client) waitEnd(t *testing.T) error {
	t.Helper()
	select {
	case err := <-cl.errc:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not end")
	}
	return nil
}

// ---------------------------------------------------------------------------
// route table
// ---------------------------------------------------------------------------

func TestTableEpochRules(t *testing.T) {
	tb := NewTable()
	up := func(epoch uint64, addr string) ApplyResult {
		t.Helper()
		res, err := tb.Apply(Update{MatchID: "m", Epoch: epoch, PrimaryAddr: addr})
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if _, ok := tb.Lookup("m"); ok {
		t.Fatal("empty table returned a route")
	}
	steps := []struct {
		epoch uint64
		addr  string
		want  ApplyResult
		route Route
	}{
		{5, "10.0.0.1:80", Applied, Route{5, "10.0.0.1:80"}},
		{4, "10.0.0.2:80", Stale, Route{5, "10.0.0.1:80"}},
		{5, "10.0.0.1:80", Duplicate, Route{5, "10.0.0.1:80"}},
		{5, "10.0.0.3:80", Conflict, Route{5, "10.0.0.1:80"}},
		{6, "10.0.0.2:80", Applied, Route{6, "10.0.0.2:80"}},
		{0, "10.0.0.9:80", Stale, Route{6, "10.0.0.2:80"}},
	}
	for i, s := range steps {
		if got := up(s.epoch, s.addr); got != s.want {
			t.Fatalf("step %d: result %q, want %q", i, got, s.want)
		}
		if r, _ := tb.Lookup("m"); r != s.route {
			t.Fatalf("step %d: route %+v, want %+v", i, r, s.route)
		}
	}
}

func TestTableFirstUpdateMayHaveEpochZero(t *testing.T) {
	tb := NewTable()
	if res, _ := tb.Apply(Update{MatchID: "m", Epoch: 0, PrimaryAddr: "h:1"}); res != Applied {
		t.Fatalf("epoch 0 for an unknown match = %q, want applied", res)
	}
}

func TestTableValidation(t *testing.T) {
	tb := NewTable()
	bad := []Update{
		{"", 1, "h:1"},
		{strings.Repeat("a", MaxMatchIDLen+1), 1, "h:1"},
		{"has space", 1, "h:1"},
		{"slash/id", 1, "h:1"},
		{"ok", 1, ""},
		{"ok", 1, "nohostport"},
		{"ok", 1, ":80"},
		{"ok", 1, "h:0"},
		{"ok", 1, "h:99999"},
		{"ok", 1, "h:abc"},
		{"ok", 1, "http://h:80"},
		{"ok", 1, "u@h:80"},
		{"ok", 1, "h/path:80"},
		{"ok", 1, "h :80"},
	}
	for _, u := range bad {
		if _, err := tb.Apply(u); err == nil {
			t.Errorf("Apply(%+v) succeeded, want an error", u)
		}
	}
	good := []Update{
		{"a", 1, "127.0.0.1:80"},
		{"A_b-9", 1, "game-0.game.svc.cluster.local:8080"},
		{strings.Repeat("z", MaxMatchIDLen), 1, "[::1]:80"},
	}
	for _, u := range good {
		if _, err := tb.Apply(u); err != nil {
			t.Errorf("Apply(%+v): %v", u, err)
		}
	}
	if len(tb.Snapshot()) != len(good) {
		t.Fatalf("invalid updates leaked into the table: %v", tb.Snapshot())
	}
}

func TestTableDefaultRoute(t *testing.T) {
	tb := NewTable()
	if err := tb.SetDefault("not an addr"); err == nil {
		t.Fatal("SetDefault accepted an invalid address")
	}
	if err := tb.SetDefault("h:1"); err != nil {
		t.Fatal(err)
	}
	if r, ok := tb.Lookup("anything"); !ok || r != (Route{0, "h:1"}) {
		t.Fatalf("default route = %+v, %v", r, ok)
	}
	_, _ = tb.Apply(Update{MatchID: "m", Epoch: 3, PrimaryAddr: "h:2"})
	if r, _ := tb.Lookup("m"); r.Addr != "h:2" {
		t.Fatalf("explicit route does not override the default: %+v", r)
	}
	_ = tb.SetDefault("")
	if _, ok := tb.Lookup("other"); ok {
		t.Fatal("cleared default still routes")
	}
}

func TestTableWatcher(t *testing.T) {
	tb := NewTable()
	w := tb.Watch("m")
	other := tb.Watch("other")
	defer other.Stop()

	for e := uint64(1); e <= 3; e++ {
		if _, err := tb.Apply(Update{MatchID: "m", Epoch: e, PrimaryAddr: "h:1"}); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-w.C():
	default:
		t.Fatal("watcher was not signalled")
	}
	select {
	case <-w.C():
		t.Fatal("three updates should coalesce into one signal")
	default:
	}
	select {
	case <-other.C():
		t.Fatal("watcher for another match was signalled")
	default:
	}
	// Stale and duplicate updates must not wake sessions.
	_, _ = tb.Apply(Update{MatchID: "m", Epoch: 2, PrimaryAddr: "h:1"})
	_, _ = tb.Apply(Update{MatchID: "m", Epoch: 3, PrimaryAddr: "h:1"})
	select {
	case <-w.C():
		t.Fatal("ignored updates signalled the watcher")
	default:
	}

	w.Stop()
	_, _ = tb.Apply(Update{MatchID: "m", Epoch: 9, PrimaryAddr: "h:1"})
	select {
	case <-w.C():
		t.Fatal("stopped watcher was signalled")
	default:
	}
	tb.mu.RLock()
	defer tb.mu.RUnlock()
	if _, ok := tb.watchers["m"]; ok {
		t.Fatal("stopped watcher was not removed from the table")
	}
}

func TestTableConcurrentApplyKeepsHighestEpoch(t *testing.T) {
	tb := NewTable()
	w := tb.Watch("m")
	defer w.Stop()

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for e := 1; e <= 200; e++ {
				epoch := uint64((e*7 + g*13) % 500)
				_, _ = tb.Apply(Update{MatchID: "m", Epoch: epoch, PrimaryAddr: fmt.Sprintf("h:%d", 1000+int(epoch))})
				tb.Lookup("m")
			}
		}(g)
	}
	wg.Wait()
	r, _ := tb.Lookup("m")
	if r.Epoch != 499 || r.Addr != "h:1499" {
		t.Fatalf("final route %+v, want epoch 499 -> h:1499", r)
	}
}

// ---------------------------------------------------------------------------
// forwarding
// ---------------------------------------------------------------------------

func TestForwardsTextAndBinaryInOrder(t *testing.T) {
	checkGoroutines(t)
	r := newRig(t, nil)
	a := newFakeGame(t, "A")
	r.route(t, "m1", 7, a)
	cl := r.connect(t, "m1")

	for i := 0; i < 200; i++ {
		cl.send(t, fmt.Sprintf("msg-%d", i))
	}
	for i := 0; i < 200; i++ {
		cl.expect(t, fmt.Sprintf("A|msg-%d", i))
	}

	payload := bytes.Repeat([]byte{0, 1, 2, 255}, 5000)
	if err := cl.c.Write(context.Background(), websocket.MessageBinary, payload); err != nil {
		t.Fatal(err)
	}
	if got := cl.next(t); got != "A|"+string(payload) {
		t.Fatalf("binary round trip corrupted (%d bytes back)", len(got))
	}

	req := a.req(0)
	if req.Path != "/ws" || req.Query != "match=m1&player=alice" {
		t.Fatalf("upstream saw path %q query %q; the query must be forwarded unchanged", req.Path, req.Query)
	}
	if req.Header.Get("X-Arcade-Match") != "m1" || req.Header.Get("X-Arcade-Epoch") != "7" {
		t.Fatalf("upstream headers: match=%q epoch=%q", req.Header.Get("X-Arcade-Match"), req.Header.Get("X-Arcade-Epoch"))
	}
	if req.Header.Get("X-Forwarded-For") != "127.0.0.1" {
		t.Fatalf("X-Forwarded-For = %q", req.Header.Get("X-Forwarded-For"))
	}
}

func TestFallbackRouteIsUsedWhenMatchHasNone(t *testing.T) {
	r := newRig(t, nil)
	a := newFakeGame(t, "A")
	if err := r.p.Table().SetDefault(a.Addr()); err != nil {
		t.Fatal(err)
	}
	cl := r.connect(t, "anything")
	cl.send(t, "x")
	cl.expect(t, "A|x")
	if got := a.req(0).Header.Get("X-Arcade-Epoch"); got != "0" {
		t.Fatalf("epoch for the default route = %q, want 0", got)
	}
}

func TestRejectsBadClientRequests(t *testing.T) {
	r := newRig(t, nil)
	for path, want := range map[string]int{
		"/ws":                                  http.StatusBadRequest,
		"/ws?match=":                           http.StatusBadRequest,
		"/ws?match=has%20space":                http.StatusBadRequest,
		"/ws?match=" + strings.Repeat("x", 65): http.StatusBadRequest,
		"/ws?match=ok":                         http.StatusUpgradeRequired, // valid id, but not a WebSocket request
	} {
		resp, err := http.Get(r.ts.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != want {
			t.Errorf("GET %s = %d, want %d", path, resp.StatusCode, want)
		}
	}
}

// dialStatus dials the proxy and reports the HTTP status of the handshake
// answer (101 on success). coder/websocket returns the response alongside the
// error when the server refuses the upgrade.
func dialStatus(url string, h http.Header) (int, error) {
	c, resp, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{HTTPHeader: h})
	if err == nil {
		_ = c.CloseNow()
		return http.StatusSwitchingProtocols, nil
	}
	if resp != nil {
		return resp.StatusCode, err
	}
	return 0, err
}

func TestClientOriginPolicy(t *testing.T) {
	r := newRig(t, func(c *Config) { c.OriginPatterns = []string{"game.example.com"} })
	a := newFakeGame(t, "A")
	_ = r.p.Table().SetDefault(a.Addr())

	origin := func(o string) http.Header {
		h := http.Header{}
		h.Set("Origin", o)
		return h
	}
	if status, err := dialStatus(r.url("m"), origin("https://game.example.com")); err != nil {
		t.Fatalf("allowed origin rejected (HTTP %d): %v", status, err)
	}
	if status, err := dialStatus(r.url("m"), origin("https://evil.example")); err == nil || status != http.StatusForbidden {
		t.Fatalf("foreign origin: status %d, err %v; want 403", status, err)
	}
}

// ---------------------------------------------------------------------------
// takeover
// ---------------------------------------------------------------------------

func TestTakeoverKeepsClientSocketOpen(t *testing.T) {
	checkGoroutines(t)
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	r.route(t, "m1", 1, a)
	cl := r.connect(t, "m1")

	cl.send(t, "one")
	cl.expect(t, "A|one")

	r.route(t, "m1", 2, b)
	eventually(t, "the session to move to B", func() bool { return r.p.Stats().Retargets.Load() == 1 && b.numReqs() == 1 })

	cl.send(t, "two")
	cl.expect(t, "B|two")
	cl.expectQuiet(t, 100*time.Millisecond) // same client socket throughout: no error, no close

	if got := b.req(0).Header.Get("X-Arcade-Epoch"); got != "2" {
		t.Fatalf("B was told epoch %q, want 2", got)
	}
	// The old primary lost its connection abruptly: it was revoked, not asked nicely.
	eventually(t, "A to notice it was dropped", func() bool { return len(a.codes()) == 1 })
	if c := a.codes()[0]; c != -1 {
		t.Fatalf("A saw close status %d, want an abrupt drop (-1)", c)
	}
}

func TestTakeoverAlsoHandlesReconnectToSameAddressWithNewEpoch(t *testing.T) {
	r := newRig(t, nil)
	a := newFakeGame(t, "A")
	r.route(t, "m1", 1, a)
	cl := r.connect(t, "m1")
	cl.send(t, "x")
	cl.expect(t, "A|x")

	r.route(t, "m1", 2, a) // same pod re-promoted under a new epoch
	eventually(t, "a second upstream connection", func() bool { return a.numReqs() == 2 })
	cl.send(t, "y")
	cl.expect(t, "A|y")
	if got := a.req(1).Header.Get("X-Arcade-Epoch"); got != "2" {
		t.Fatalf("second connection carried epoch %q, want 2", got)
	}
}

func TestCrashBuffersClientMessagesThenReplaysThemInOrder(t *testing.T) {
	checkGoroutines(t)
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	r.route(t, "m1", 1, a)
	cl := r.connect(t, "m1")
	cl.send(t, "before")
	cl.expect(t, "A|before")
	// "before" itself may have been queued for a moment while the first
	// upstream was still connecting, so count only what the crash adds.
	baseline := r.p.Stats().BufferedMsgs.Load()

	a.Kill()
	// Once the proxy fails to redial A it knows the match has no primary, so
	// everything sent from here on is queued rather than written to a corpse.
	eventually(t, "the proxy to notice A is gone", func() bool { return r.p.Stats().DialFailures.Load() >= 1 })

	for i := 1; i <= 5; i++ {
		cl.send(t, "m"+strconv.Itoa(i))
	}
	cl.expectQuiet(t, 150*time.Millisecond)
	if got := r.p.Stats().BufferedMsgs.Load() - baseline; got != 5 {
		t.Fatalf("buffered %d messages after the crash, want 5", got)
	}

	r.route(t, "m1", 2, b)
	for i := 1; i <= 5; i++ {
		cl.expect(t, "B|m"+strconv.Itoa(i))
	}
	cl.send(t, "after")
	cl.expect(t, "B|after")
	cl.expectQuiet(t, 100*time.Millisecond)
}

func TestStaleAndDuplicateRouteUpdatesDoNotMoveClients(t *testing.T) {
	r := newRig(t, nil)
	a, b, c := newFakeGame(t, "A"), newFakeGame(t, "B"), newFakeGame(t, "C")
	r.route(t, "m1", 5, a)
	cl := r.connect(t, "m1")
	cl.send(t, "1")
	cl.expect(t, "A|1")

	for _, u := range []Update{
		{"m1", 4, b.Addr()}, // older epoch
		{"m1", 5, a.Addr()}, // duplicate
		{"m1", 5, c.Addr()}, // conflict
	} {
		if _, err := r.p.ApplyUpdate(u); err != nil {
			t.Fatal(err)
		}
	}
	cl.send(t, "2")
	cl.expect(t, "A|2")
	if b.numReqs()+c.numReqs() != 0 || a.numReqs() != 1 {
		t.Fatalf("ignored updates caused connections: A=%d B=%d C=%d", a.numReqs(), b.numReqs(), c.numReqs())
	}
	if got := r.p.Stats().Retargets.Load(); got != 0 {
		t.Fatalf("retargets = %d, want 0", got)
	}

	r.route(t, "m1", 9, b)
	eventually(t, "move to B", func() bool { return r.p.Stats().Retargets.Load() == 1 })
	if _, err := r.p.ApplyUpdate(Update{"m1", 7, c.Addr()}); err != nil { // late, out-of-order
		t.Fatal(err)
	}
	cl.send(t, "3")
	cl.expect(t, "B|3")
	if c.numReqs() != 0 {
		t.Fatal("a stale update moved the client to C")
	}
}

func TestOnlyTheAffectedMatchMoves(t *testing.T) {
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	r.route(t, "m1", 1, a)
	r.route(t, "m2", 1, a)
	c1, c2 := r.connect(t, "m1"), r.connect(t, "m2")
	c1.send(t, "x")
	c1.expect(t, "A|x")
	c2.send(t, "x")
	c2.expect(t, "A|x")

	r.route(t, "m1", 2, b)
	eventually(t, "m1 to move", func() bool { return r.p.Stats().Retargets.Load() == 1 })
	c1.send(t, "y")
	c1.expect(t, "B|y")
	c2.send(t, "y")
	c2.expect(t, "A|y")
	if a.numReqs() != 2 {
		t.Fatalf("m2 reconnected to A (%d handshakes), it should have been left alone", a.numReqs())
	}
}

func TestRouteChangeAbortsADialToAnUnresponsivePrimary(t *testing.T) {
	r := newRig(t, func(c *Config) { c.DialTimeout = 10 * time.Second })
	b := newFakeGame(t, "B")

	// Accepts TCP, never answers the WebSocket handshake.
	hole, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var held []net.Conn
	var heldMu sync.Mutex
	go func() {
		for {
			c, err := hole.Accept()
			if err != nil {
				return
			}
			heldMu.Lock()
			held = append(held, c)
			heldMu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = hole.Close()
		heldMu.Lock()
		for _, c := range held {
			_ = c.Close()
		}
		heldMu.Unlock()
	})

	if _, err := r.p.ApplyUpdate(Update{"m1", 1, hole.Addr().String()}); err != nil {
		t.Fatal(err)
	}
	cl := r.connect(t, "m1")
	cl.send(t, "hello") // queued: nobody to send it to yet
	eventually(t, "the proxy to start dialing the black hole", func() bool {
		heldMu.Lock()
		defer heldMu.Unlock()
		return len(held) == 1
	})

	start := time.Now()
	r.route(t, "m1", 2, b)
	cl.expect(t, "B|hello")
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("takeover took %v; the pending dial should have been abandoned at once (DialTimeout is 10s)", d)
	}
}

// ---------------------------------------------------------------------------
// the fence
// ---------------------------------------------------------------------------

func newWSPair(t *testing.T) (server, client *websocket.Conn) {
	t.Helper()
	ch := make(chan *websocket.Conn, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, err := websocket.Accept(w, r, nil); err == nil {
			ch <- c
		}
	}))
	t.Cleanup(ts.Close)
	c, _, err := websocket.Dial(context.Background(), "ws"+strings.TrimPrefix(ts.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	s := <-ch
	t.Cleanup(func() { _ = s.CloseNow() })
	return s, c
}

func TestFenceDropsMessagesFromARevokedUpstream(t *testing.T) {
	p := New(Config{})
	srvSide, peer := newWSPair(t)
	s := newSession(p, srvSide, "m", "", "127.0.0.1")
	oldUp, newUp := &upstream{}, &upstream{}

	s.cur = newUp
	if s.deliver(oldUp, websocket.MessageText, []byte("from the zombie")) {
		t.Fatal("deliver accepted a message from an upstream that is not current")
	}
	if got := p.Stats().FencedDrops.Load(); got != 1 {
		t.Fatalf("FencedDrops = %d, want 1", got)
	}
	if !s.deliver(newUp, websocket.MessageText, []byte("legit")) {
		t.Fatal("deliver refused the current upstream")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, data, err := peer.Read(ctx)
	if err != nil || string(data) != "legit" {
		t.Fatalf("client received %q, %v; want only %q", data, err, "legit")
	}

	// Revoking the current upstream closes the gate for it too.
	s.fence(newUp)
	if s.deliver(newUp, websocket.MessageText, []byte("late")) {
		t.Fatal("deliver accepted a message after the upstream was fenced")
	}
	if got := p.Stats().FencedDrops.Load(); got != 2 {
		t.Fatalf("FencedDrops = %d, want 2", got)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel2()
	if _, d, err := peer.Read(ctx2); err == nil {
		t.Fatalf("client received %q after the fence", d)
	}
}

// End to end: a revoked primary that is still alive and chatty must not reach
// the client once the route has moved.
func TestZombiePrimaryCannotTalkToClientsAfterTakeover(t *testing.T) {
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	r.route(t, "m1", 1, a)
	cl := r.connect(t, "m1")
	cl.send(t, "x")
	cl.expect(t, "A|x")

	r.route(t, "m1", 2, b)
	eventually(t, "move to B", func() bool { return r.p.Stats().Retargets.Load() == 1 })

	for i := 0; i < 20; i++ {
		a.Send("zombie state update") // A's sockets are already gone; nothing may leak through
		time.Sleep(2 * time.Millisecond)
	}
	b.Send("B pushes state")
	cl.expect(t, "B pushes state")
	cl.expectQuiet(t, 100*time.Millisecond)
}

// ---------------------------------------------------------------------------
// failure handling
// ---------------------------------------------------------------------------

func TestClientIsClosedWith1013AfterTheOutageCap(t *testing.T) {
	checkGoroutines(t)
	t.Run("no route at all", func(t *testing.T) {
		r := newRig(t, func(c *Config) { c.MaxOutage = 300 * time.Millisecond })
		cl := r.connect(t, "m1")
		start := time.Now()
		err := cl.waitEnd(t)
		if websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
			t.Fatalf("client ended with %v, want close 1013", err)
		}
		if d := time.Since(start); d < 250*time.Millisecond || d > 2*time.Second {
			t.Fatalf("closed after %v, want about 300ms", d)
		}
		if got := r.p.Stats().OutageTimeouts.Load(); got != 1 {
			t.Fatalf("OutageTimeouts = %d", got)
		}
	})
	t.Run("route points at a dead pod", func(t *testing.T) {
		r := newRig(t, func(c *Config) { c.MaxOutage = 300 * time.Millisecond })
		a := newFakeGame(t, "A")
		r.route(t, "m1", 1, a)
		a.Kill()
		cl := r.connect(t, "m1")
		if err := cl.waitEnd(t); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
			t.Fatalf("client ended with %v, want close 1013", err)
		}
	})
	t.Run("outage after a working connection", func(t *testing.T) {
		r := newRig(t, func(c *Config) { c.MaxOutage = 300 * time.Millisecond })
		a := newFakeGame(t, "A")
		r.route(t, "m1", 1, a)
		cl := r.connect(t, "m1")
		cl.send(t, "x")
		cl.expect(t, "A|x")
		a.Kill()
		if err := cl.waitEnd(t); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
			t.Fatalf("client ended with %v, want close 1013", err)
		}
	})
}

func TestRecoveryWithinTheOutageCapKeepsTheClient(t *testing.T) {
	r := newRig(t, func(c *Config) { c.MaxOutage = 2 * time.Second })
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	r.route(t, "m1", 1, a)
	cl := r.connect(t, "m1")
	cl.send(t, "x")
	cl.expect(t, "A|x")
	a.Kill()
	time.Sleep(700 * time.Millisecond) // most of the budget
	r.route(t, "m1", 2, b)
	cl.send(t, "y")
	cl.expect(t, "B|y")
}

func TestPendingQueueOverflowClosesTheClient(t *testing.T) {
	r := newRig(t, func(c *Config) { c.PendingMaxMessages = 3; c.MaxOutage = 10 * time.Second })
	cl := r.connect(t, "m1") // no route: everything queues
	for i := 0; i < 4; i++ {
		cl.send(t, "q")
	}
	if err := cl.waitEnd(t); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
		t.Fatalf("client ended with %v, want close 1013", err)
	}
	if got := r.p.Stats().BufferOverflows.Load(); got != 1 {
		t.Fatalf("BufferOverflows = %d", got)
	}
}

func TestPendingQueueByteLimit(t *testing.T) {
	r := newRig(t, func(c *Config) { c.PendingMaxBytes = 100; c.MaxOutage = 10 * time.Second })
	cl := r.connect(t, "m1")
	cl.send(t, strings.Repeat("x", 60))
	cl.expectQuiet(t, 50*time.Millisecond)
	cl.send(t, strings.Repeat("x", 60))
	if err := cl.waitEnd(t); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
		t.Fatalf("client ended with %v, want close 1013", err)
	}
}

func TestUpstreamClosingNormallyEndsTheClientSession(t *testing.T) {
	checkGoroutines(t)
	for _, tc := range []struct {
		name   string
		code   websocket.StatusCode
		reason string
	}{
		{"normal", websocket.StatusNormalClosure, "game over"},
		{"policy", websocket.StatusPolicyViolation, "not your turn too many times"},
		{"app code", 4001, "kicked"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t, nil)
			a := newFakeGame(t, "A")
			r.route(t, "m1", 1, a)
			cl := r.connect(t, "m1")
			cl.send(t, "x")
			cl.expect(t, "A|x")

			a.CloseAll(tc.code, tc.reason)
			err := cl.waitEnd(t)
			var ce websocket.CloseError
			if !errors.As(err, &ce) || ce.Code != tc.code || ce.Reason != tc.reason {
				t.Fatalf("client saw %v, want close %d %q", err, tc.code, tc.reason)
			}
			eventually(t, "session to be released", func() bool { return r.p.Sessions() == 0 })
			if a.numReqs() != 1 {
				t.Fatalf("proxy reconnected %d times after a terminal close", a.numReqs()-1)
			}
		})
	}
}

func TestUpstreamSteppingDownIsHiddenFromTheClient(t *testing.T) {
	for _, code := range []websocket.StatusCode{websocket.StatusGoingAway, websocket.StatusServiceRestart, websocket.StatusTryAgainLater, websocket.StatusInternalError} {
		t.Run(strconv.Itoa(int(code)), func(t *testing.T) {
			r := newRig(t, nil)
			a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
			r.route(t, "m1", 1, a)
			cl := r.connect(t, "m1")
			cl.send(t, "x")
			cl.expect(t, "A|x")

			a.setStatus(http.StatusServiceUnavailable) // a demoted pod refuses new connections
			a.CloseAll(code, "stepping down")
			cl.expectQuiet(t, 100*time.Millisecond) // the client must not see this
			r.route(t, "m1", 2, b)
			cl.send(t, "y")
			cl.expect(t, "B|y")
		})
	}
}

func TestHandshakeRejectionByGamePod(t *testing.T) {
	t.Run("403 is final", func(t *testing.T) {
		r := newRig(t, nil)
		a := newFakeGame(t, "A")
		a.setStatus(http.StatusForbidden)
		r.route(t, "m1", 1, a)
		cl := r.connect(t, "m1")
		err := cl.waitEnd(t)
		var ce websocket.CloseError
		if !errors.As(err, &ce) || ce.Code != websocket.StatusPolicyViolation || !strings.Contains(ce.Reason, "403") {
			t.Fatalf("client saw %v, want close 1008 mentioning 403", err)
		}
		if a.numReqs() != 1 {
			t.Fatalf("proxy retried a 403 (%d handshakes)", a.numReqs())
		}
	})
	t.Run("503 is retried until it works", func(t *testing.T) {
		r := newRig(t, nil)
		a := newFakeGame(t, "A")
		a.failFirst = 4
		r.route(t, "m1", 1, a)
		cl := r.connect(t, "m1")
		cl.send(t, "x")
		cl.expect(t, "A|x")
		if a.numReqs() != 5 {
			t.Fatalf("handshakes = %d, want 5 (4 refused + 1 accepted)", a.numReqs())
		}
	})
}

// A game pod that answers the upgrade with a redirect must not be followed:
// the route table, not the pod, decides where clients go.
func TestDialDoesNotFollowRedirects(t *testing.T) {
	r := newRig(t, func(c *Config) { c.MaxOutage = 600 * time.Millisecond })
	b := newFakeGame(t, "B")
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		http.Redirect(w, req, "http://"+b.Addr()+"/ws", http.StatusFound)
	}))
	t.Cleanup(redirector.Close)

	addr := strings.TrimPrefix(redirector.URL, "http://")
	if _, err := r.p.ApplyUpdate(Update{MatchID: "m1", Epoch: 1, PrimaryAddr: addr}); err != nil {
		t.Fatal(err)
	}
	cl := r.connect(t, "m1")
	if err := cl.waitEnd(t); websocket.CloseStatus(err) != websocket.StatusTryAgainLater {
		t.Fatalf("client ended with %v, want 1013 after the outage cap", err)
	}
	if n := b.numReqs(); n != 0 {
		t.Fatalf("the redirect target received %d requests; redirects must not be followed", n)
	}
	if r.p.Stats().DialFailures.Load() == 0 {
		t.Fatal("a redirect answer should count as a failed dial")
	}
}

// Game pods are reached directly, whatever HTTP_PROXY says.
func TestUpstreamDialerIgnoresProxyEnvironment(t *testing.T) {
	tr, ok := New(Config{}).httpClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("upstream transport is %T, want *http.Transport", New(Config{}).httpClient.Transport)
	}
	if tr.Proxy != nil {
		t.Fatal("the upstream transport consults proxy environment variables")
	}
}

func TestUpstreamThatKeepsDroppingTheConnectionIsGivenUpOn(t *testing.T) {
	r := newRig(t, func(c *Config) { c.MaxOutage = 30 * time.Second })
	a := newFakeGame(t, "A")
	a.flap = true // every connection is accepted and immediately closed with 1012
	r.route(t, "m1", 1, a)

	cl := r.connect(t, "m1")
	err := cl.waitEnd(t)
	if got := websocket.CloseStatus(err); got != websocket.StatusBadGateway {
		t.Fatalf("client ended with %v, want close 1014", err)
	}
	if n := a.numReqs(); n < maxQuickFailures || n > maxQuickFailures+2 {
		t.Fatalf("proxy made %d attempts, want about %d", n, maxQuickFailures)
	}
}

func TestClientDisconnectIsPassedToTheGamePod(t *testing.T) {
	checkGoroutines(t)
	t.Run("clean close", func(t *testing.T) {
		r := newRig(t, nil)
		a := newFakeGame(t, "A")
		r.route(t, "m1", 1, a)
		cl := r.connect(t, "m1")
		cl.send(t, "x")
		cl.expect(t, "A|x")
		if err := cl.c.Close(websocket.StatusNormalClosure, "bye"); err != nil {
			t.Fatal(err)
		}
		eventually(t, "A to see the disconnect", func() bool { return len(a.codes()) == 1 })
		if c := a.codes()[0]; c != websocket.StatusNormalClosure {
			t.Fatalf("A saw close %d, want 1000", c)
		}
		eventually(t, "session cleanup", func() bool { return r.p.Sessions() == 0 && r.p.Stats().ActiveSessions.Load() == 0 })
	})
	t.Run("abrupt drop", func(t *testing.T) {
		r := newRig(t, nil)
		a := newFakeGame(t, "A")
		r.route(t, "m1", 1, a)
		cl := r.connect(t, "m1")
		cl.send(t, "x")
		cl.expect(t, "A|x")
		_ = cl.c.CloseNow()
		eventually(t, "A to see the disconnect", func() bool { return len(a.codes()) == 1 })
		if c := a.codes()[0]; c != websocket.StatusGoingAway {
			t.Fatalf("A saw close %d, want 1001", c)
		}
		eventually(t, "session cleanup", func() bool { return r.p.Sessions() == 0 })
	})
	t.Run("while waiting for a primary", func(t *testing.T) {
		r := newRig(t, nil)
		cl := r.connect(t, "m1")
		cl.send(t, "queued")
		_ = cl.c.CloseNow()
		eventually(t, "session cleanup", func() bool { return r.p.Sessions() == 0 })
	})
}

func TestUpstreamPingsDetectASilentPrimary(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.UpstreamPingInterval = 40 * time.Millisecond
		c.PingTimeout = 80 * time.Millisecond
	})
	a := newFakeGame(t, "A")
	a.mute = true  // completes the handshake, then never reads or answers
	a.maxConns = 1 // a second attempt is refused, so a drop becomes visible
	r.route(t, "m1", 1, a)
	cl := r.connect(t, "m1")

	eventually(t, "the proxy to give up on the silent primary and fail to redial", func() bool {
		return r.p.Stats().DialFailures.Load() >= 1
	})
	cl.expectQuiet(t, 50*time.Millisecond) // the client is still connected, just waiting
}

func TestClientPingsDropAClientThatVanished(t *testing.T) {
	r := newRig(t, func(c *Config) {
		c.ClientPingInterval = 40 * time.Millisecond
		c.PingTimeout = 80 * time.Millisecond
	})
	a := newFakeGame(t, "A")
	r.route(t, "m1", 1, a)

	// A raw client that completes the handshake and then never reads again.
	nc, err := net.Dial("tcp", strings.TrimPrefix(r.ts.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = nc.Close() }()
	_, _ = io.WriteString(nc, "GET /ws?match=m1 HTTP/1.1\r\nHost: x\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	eventually(t, "session start", func() bool { return r.p.Sessions() == 1 })
	eventually(t, "the idle client to be dropped", func() bool { return r.p.Sessions() == 0 })
}

func TestShutdown(t *testing.T) {
	checkGoroutines(t)
	r := newRig(t, nil)
	a := newFakeGame(t, "A")
	r.route(t, "m1", 1, a)
	cls := []*client{r.connect(t, "m1"), r.connect(t, "m1"), r.connect(t, "m1")}
	for _, cl := range cls {
		cl.send(t, "x")
		cl.expect(t, "A|x")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for i, cl := range cls {
		var ce websocket.CloseError
		if err := cl.waitEnd(t); !errors.As(err, &ce) || ce.Code != websocket.StatusGoingAway {
			t.Fatalf("client %d saw %v, want close 1001", i, err)
		}
	}
	if r.p.Sessions() != 0 {
		t.Fatal("sessions remain after Shutdown")
	}
	if status, err := dialStatus(r.url("m1"), nil); err == nil || status != http.StatusServiceUnavailable {
		t.Fatalf("dial after Shutdown: status %d, err %v; want 503", status, err)
	}
}

func TestMaxSessions(t *testing.T) {
	r := newRig(t, func(c *Config) { c.MaxSessions = 1 })
	a := newFakeGame(t, "A")
	_ = r.p.Table().SetDefault(a.Addr())
	first := r.connect(t, "m1")
	first.send(t, "x")
	first.expect(t, "A|x")

	if status, err := dialStatus(r.url("m2"), nil); err == nil || status != http.StatusServiceUnavailable {
		t.Fatalf("second client: status %d, err %v; want 503", status, err)
	}
	_ = first.c.Close(websocket.StatusNormalClosure, "")
	eventually(t, "slot to free up", func() bool { return r.p.Sessions() == 0 })
	second := r.connect(t, "m2")
	second.send(t, "y")
	second.expect(t, "A|y")
}

// ---------------------------------------------------------------------------
// unit tests of helpers
// ---------------------------------------------------------------------------

func TestClassifyUpstreamEnd(t *testing.T) {
	ce := func(c websocket.StatusCode) error { return websocket.CloseError{Code: c, Reason: "r"} }
	cases := []struct {
		err      error
		terminal bool
		code     websocket.StatusCode
	}{
		{io.EOF, false, 0},
		{io.ErrUnexpectedEOF, false, 0},
		{errors.New("connection reset by peer"), false, 0},
		{ce(websocket.StatusGoingAway), false, 0},
		{ce(websocket.StatusServiceRestart), false, 0},
		{ce(websocket.StatusTryAgainLater), false, 0},
		{ce(websocket.StatusInternalError), false, 0},
		{ce(websocket.StatusNoStatusRcvd), false, 0},
		{ce(websocket.StatusNormalClosure), true, websocket.StatusNormalClosure},
		{ce(websocket.StatusPolicyViolation), true, websocket.StatusPolicyViolation},
		{ce(websocket.StatusMessageTooBig), true, websocket.StatusMessageTooBig},
		{ce(3000), true, 3000},
		{ce(4999), true, 4999},
		{ce(websocket.StatusProtocolError), true, websocket.StatusInternalError},
		{ce(websocket.StatusInvalidFramePayloadData), true, websocket.StatusInternalError},
		{fmt.Errorf("wrapped: %w", ce(websocket.StatusNormalClosure)), true, websocket.StatusNormalClosure},
	}
	for i, c := range cases {
		term, code, _ := classifyUpstreamEnd(c.err)
		if term != c.terminal || (term && code != c.code) {
			t.Errorf("case %d (%v): terminal=%v code=%d, want terminal=%v code=%d", i, c.err, term, code, c.terminal, c.code)
		}
	}
}

func TestBackoffStaysWithinBounds(t *testing.T) {
	p := New(Config{BackoffMin: 10 * time.Millisecond, BackoffMax: 200 * time.Millisecond})
	s := newSession(p, nil, "m", "", "x")

	span := func(n int) (lo, hi time.Duration) {
		lo = time.Hour
		for i := 0; i < 500; i++ {
			d := s.backoff(n)
			lo, hi = min(lo, d), max(hi, d)
		}
		return lo, hi
	}
	for n := 0; n < 40; n++ {
		lo, hi := span(n)
		if lo < 5*time.Millisecond || hi > 200*time.Millisecond {
			t.Fatalf("attempt %d: delays in [%v, %v], outside [5ms, 200ms]", n, lo, hi)
		}
	}
	if _, hi := span(0); hi > 10*time.Millisecond {
		t.Fatalf("first delay up to %v, want at most BackoffMin (10ms)", hi)
	}
	if lo, hi := span(30); lo < 100*time.Millisecond || hi < 190*time.Millisecond {
		t.Fatalf("late delays in [%v, %v], want to sit just under the 200ms cap", lo, hi)
	}
}

// ---------------------------------------------------------------------------
// admin API
// ---------------------------------------------------------------------------

func adminDo(h http.Handler, method, path, body, token string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAdminAPI(t *testing.T) {
	p := New(Config{})
	h := p.AdminHandler()

	post := func(body string) *httptest.ResponseRecorder { return adminDo(h, "POST", "/admin/route", body, "") }
	check := func(rec *httptest.ResponseRecorder, status int, contains string) {
		t.Helper()
		if rec.Code != status || !strings.Contains(rec.Body.String(), contains) {
			t.Fatalf("status %d body %q; want %d containing %q", rec.Code, rec.Body.String(), status, contains)
		}
	}

	check(post(`{"match_id":"m1","epoch":2,"primary_addr":"10.0.0.1:8080"}`), 200, `"applied"`)
	check(post(`{"match_id":"m1","epoch":1,"primary_addr":"10.0.0.2:8080"}`), 200, `"stale"`)
	check(post(`{"match_id":"m1","epoch":2,"primary_addr":"10.0.0.1:8080"}`), 200, `"duplicate"`)
	check(post(`{"match_id":"m1","epoch":2,"primary_addr":"10.0.0.9:8080"}`), 409, `"conflict"`)
	check(post(`{"match_id":"m1","epoch":3,"primary_addr":"10.0.0.2:8080"}`), 200, `"applied"`)

	check(post(`{"match_id":"bad id","epoch":1,"primary_addr":"h:1"}`), 400, "match_id")
	check(post(`{"match_id":"m","epoch":1,"primary_addr":"nope"}`), 400, "host:port")
	check(post(`{"match_id":"m","epoch":-1,"primary_addr":"h:1"}`), 400, "")
	check(post(`{"match_id":"m","epoch":1,"primary_addr":"h:1","extra":true}`), 400, "unknown field")
	check(post(`not json`), 400, "")
	check(post(`{"match_id":"m","epoch":1,"primary_addr":"h:1"} {"x":1}`), 400, "trailing")
	check(post(`{"match_id":"m","epoch":1,"primary_addr":"`+strings.Repeat("a", 5000)+`:1"}`), 400, "")

	if rt, _ := p.Table().Lookup("m1"); rt != (Route{3, "10.0.0.2:8080"}) {
		t.Fatalf("table = %+v", rt)
	}
	if _, ok := p.Table().Lookup("m"); ok {
		t.Fatal("a rejected request modified the table")
	}

	check(adminDo(h, "GET", "/admin/routes", "", ""), 200, `"m1":{"epoch":3,"primary_addr":"10.0.0.2:8080"}`)
	check(adminDo(h, "GET", "/healthz", "", ""), 200, "ok")
	check(adminDo(h, "GET", "/stats", "", ""), 200, `"routes_applied":2`)
	if rec := adminDo(h, "GET", "/admin/route", "", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET /admin/route = %d, want 405", rec.Code)
	}
	if s := p.Stats().Snapshot(); s.RoutesApplied != 2 || s.RoutesIgnored != 3 {
		t.Fatalf("stats = %+v", s)
	}
}

func TestAdminAuth(t *testing.T) {
	p := New(Config{AdminToken: "s3cret"})
	h := p.AdminHandler()
	body := `{"match_id":"m1","epoch":1,"primary_addr":"h:1"}`

	for _, tok := range []string{"", "wrong", "s3cret "} {
		if rec := adminDo(h, "POST", "/admin/route", body, tok); rec.Code != http.StatusUnauthorized {
			t.Fatalf("token %q: status %d, want 401", tok, rec.Code)
		}
	}
	req := httptest.NewRequest("POST", "/admin/route", strings.NewReader(body))
	req.Header.Set("Authorization", "s3cret") // missing the Bearer scheme
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no Bearer scheme: status %d, want 401", rec.Code)
	}
	if _, ok := p.Table().Lookup("m1"); ok {
		t.Fatal("an unauthenticated request changed the table")
	}
	if rec := adminDo(h, "POST", "/admin/route", body, "s3cret"); rec.Code != 200 {
		t.Fatalf("valid token: status %d", rec.Code)
	}
	if rec := adminDo(h, "GET", "/admin/routes", "", ""); rec.Code != http.StatusUnauthorized {
		t.Fatalf("/admin/routes without token: %d, want 401", rec.Code)
	}
	if rec := adminDo(h, "GET", "/healthz", "", ""); rec.Code != 200 {
		t.Fatalf("/healthz must stay open for probes, got %d", rec.Code)
	}
}

// Takeover driven through the real admin endpoint over HTTP.
func TestTakeoverViaAdminHTTP(t *testing.T) {
	r := newRig(t, nil)
	admin := httptest.NewServer(r.p.AdminHandler())
	t.Cleanup(admin.Close)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")

	postRoute := func(epoch int, g *fakeGame) {
		t.Helper()
		body := fmt.Sprintf(`{"match_id":"m1","epoch":%d,"primary_addr":%q}`, epoch, g.Addr())
		resp, err := http.Post(admin.URL+"/admin/route", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("admin POST: %d", resp.StatusCode)
		}
	}
	postRoute(1, a)
	cl := r.connect(t, "m1")
	cl.send(t, "x")
	cl.expect(t, "A|x")
	a.Kill()
	postRoute(2, b)
	cl.send(t, "y")
	cl.expect(t, "B|y")
}

// ---------------------------------------------------------------------------
// client connection continuity
// ---------------------------------------------------------------------------

// continuityRun is the Phase 2 scenario: many clients stay connected and keep
// playing while their primary is replaced.
func continuityRun(t *testing.T, kill bool) {
	checkGoroutines(t)
	const clients = 30
	r := newRig(t, func(c *Config) { c.MaxOutage = 10 * time.Second })
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")

	type result struct {
		mu      sync.Mutex
		sent    int
		replies []string
		broken  error
	}
	res := make([]*result, clients)
	stop := make(chan struct{})   // senders stop
	finish := make(chan struct{}) // readers stop
	var senders, readers sync.WaitGroup

	for i := 0; i < clients; i++ {
		match := fmt.Sprintf("room%d", i)
		r.route(t, match, 1, a)
		c := r.connect(t, match)
		rs := &result{}
		res[i] = rs

		readers.Add(1)
		go func() { // consume replies
			defer readers.Done()
			for {
				select {
				case m := <-c.msgs:
					rs.mu.Lock()
					rs.replies = append(rs.replies, m)
					rs.mu.Unlock()
				case err := <-c.errc:
					rs.mu.Lock()
					rs.broken = err
					rs.mu.Unlock()
					return
				case <-finish:
					// drain whatever is already queued, then finish
					for {
						select {
						case m := <-c.msgs:
							rs.mu.Lock()
							rs.replies = append(rs.replies, m)
							rs.mu.Unlock()
						default:
							return
						}
					}
				}
			}
		}()

		senders.Add(1)
		go func() {
			defer senders.Done()
			for n := 1; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := c.c.Write(ctx, websocket.MessageText, []byte(strconv.Itoa(n)))
				cancel()
				if err != nil {
					rs.mu.Lock()
					rs.broken = fmt.Errorf("write: %w", err)
					rs.mu.Unlock()
					return
				}
				rs.mu.Lock()
				rs.sent = n
				rs.mu.Unlock()
				time.Sleep(2 * time.Millisecond)
			}
		}()
	}

	time.Sleep(300 * time.Millisecond) // everyone playing against A

	if kill {
		a.Kill()
		time.Sleep(60 * time.Millisecond)
	}
	for i := 0; i < clients; i++ {
		r.route(t, fmt.Sprintf("room%d", i), 2, b)
	}
	eventually(t, "every client to move to B", func() bool {
		return r.p.Stats().Retargets.Load() == clients && b.numReqs() == clients
	})
	time.Sleep(300 * time.Millisecond) // everyone playing against B

	close(stop)
	senders.Wait()
	// Let the last replies come back, then collect.
	eventually(t, "the last message of every client to be answered by B", func() bool {
		for _, rs := range res {
			rs.mu.Lock()
			ok := len(rs.replies) > 0 && strings.HasSuffix(rs.replies[len(rs.replies)-1], "|"+strconv.Itoa(rs.sent))
			rs.mu.Unlock()
			if !ok {
				return false
			}
		}
		return true
	})
	close(finish)
	readers.Wait()

	totalSent, totalGot, lost, dups, fromB := 0, 0, 0, 0, 0
	for i, rs := range res {
		rs.mu.Lock()
		if rs.broken != nil {
			t.Errorf("client %d lost its connection: %v", i, rs.broken)
		}
		last := 0
		seen := map[int]bool{}
		for _, m := range rs.replies {
			who, num, ok := strings.Cut(m, "|")
			n, err := strconv.Atoi(num)
			if !ok || err != nil {
				t.Fatalf("client %d got garbage %q", i, m)
			}
			if n < last {
				t.Errorf("client %d: reply %d arrived after %d (reordered)", i, n, last)
			}
			last = n
			if seen[n] {
				dups++
			}
			seen[n] = true
			if who == "B" {
				fromB++
			}
		}
		if strings.HasPrefix(rs.replies[len(rs.replies)-1], "A|") {
			t.Errorf("client %d: last reply came from the old primary", i)
		}
		totalSent += rs.sent
		totalGot += len(seen)
		lost += rs.sent - len(seen)
		rs.mu.Unlock()
	}
	t.Logf("kill=%v: %d clients, %d messages sent, %d answered, %d unanswered (in flight at the cut), %d duplicate replies, %d replies from B",
		kill, clients, totalSent, totalGot, lost, dups, fromB)

	if got := r.p.Stats().Retargets.Load(); got != clients {
		t.Errorf("retargets = %d, want %d", got, clients)
	}
	if lost > totalSent/10 {
		t.Errorf("%d of %d messages went unanswered; only the few in flight at the moment of the cut may be lost", lost, totalSent)
	}
	if r.p.Sessions() != clients {
		t.Errorf("sessions = %d, want %d", r.p.Sessions(), clients)
	}
}

func TestContinuityThroughGracefulTakeover(t *testing.T) { continuityRun(t, false) }
func TestContinuityThroughCrashAndTakeover(t *testing.T) { continuityRun(t, true) }
