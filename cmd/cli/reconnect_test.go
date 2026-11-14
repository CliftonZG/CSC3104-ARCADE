package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fastRetry makes the loops below finish in milliseconds.
var fastRetry = retryPolicy{Initial: time.Millisecond, Max: 4 * time.Millisecond, MaxOutage: 2 * time.Second}

func noLog(string, ...any) {}

// testCtx bounds a test so that a loop that wrongly never ends fails the test
// after a few seconds instead of hanging it.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func noSleep(ctx context.Context, _ time.Duration) error { return ctx.Err() }

func TestShouldRetry(t *testing.T) {
	closeErr := func(code websocket.StatusCode) error {
		// A CloseError is what conn.Read returns after a close frame.
		return websocket.CloseError{Code: code}
	}
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"our own shutdown", context.Canceled, false},
		{"network error", errors.New("read: connection reset by peer"), true},
		{"abnormal drop 1006", closeErr(websocket.StatusAbnormalClosure), true},
		{"proxy draining 1001", closeErr(websocket.StatusGoingAway), true},
		{"service restart 1012", closeErr(websocket.StatusServiceRestart), true},
		{"try again later 1013", closeErr(websocket.StatusTryAgainLater), true},
		{"bad gateway 1014", closeErr(websocket.StatusBadGateway), true},
		{"normal closure 1000", closeErr(websocket.StatusNormalClosure), false},
		{"policy violation 1008", closeErr(websocket.StatusPolicyViolation), false},
		{"message too big 1009", closeErr(websocket.StatusMessageTooBig), false},
		{"application code 3000", closeErr(3000), false},
		{"application code 4999", closeErr(4999), false},
	} {
		if got := shouldRetry(c.err); got != c.want {
			t.Errorf("%s: shouldRetry = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestHandshakeTerminal(t *testing.T) {
	for code, want := range map[int]bool{400: true, 401: true, 403: true, 404: true, 502: false, 503: false, 500: false} {
		if got := handshakeTerminal(&http.Response{StatusCode: code}); got != want {
			t.Errorf("HTTP %d: handshakeTerminal = %v, want %v", code, got, want)
		}
	}
	if handshakeTerminal(nil) {
		t.Error("no response must count as an outage, not a refusal")
	}
}

// A fake world for the loop: dial hands out connections to a local echo server
// and can be told to fail; play returns the scripted errors one by one.
type loopRig struct {
	srv       *httptest.Server
	dials     atomic.Int32
	failDials atomic.Int32 // the next N dials fail with a network error
	refuse    atomic.Int32 // HTTP status to refuse the handshake with, 0 = accept
}

func newLoopRig(t *testing.T) *loopRig {
	t.Helper()
	r := &loopRig{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		c, err := websocket.Accept(w, req, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		<-req.Context().Done()
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *loopRig) dial(ctx context.Context) (*websocket.Conn, *http.Response, error) {
	r.dials.Add(1)
	if r.failDials.Add(-1) >= 0 {
		return nil, nil, errors.New("dial tcp: connection refused")
	}
	if code := int(r.refuse.Load()); code != 0 {
		return nil, &http.Response{StatusCode: code}, errors.New("expected handshake response status code 101")
	}
	url := "ws" + strings.TrimPrefix(r.srv.URL, "http")
	return websocket.Dial(ctx, url, nil)
}

// scripted returns a play function that ends with errs[0], errs[1], ... and
// counts its calls.
func scripted(calls *atomic.Int32, errs ...error) sessionFunc {
	return func(ctx context.Context, _ *websocket.Conn) error {
		i := int(calls.Add(1)) - 1
		if i >= len(errs) {
			return websocket.CloseError{Code: websocket.StatusNormalClosure}
		}
		return errs[i]
	}
}

func TestReconnectsAfterAbnormalDrop(t *testing.T) {
	r := newLoopRig(t)
	var plays atomic.Int32

	play := scripted(&plays, websocket.CloseError{Code: websocket.StatusAbnormalClosure}, errors.New("EOF"))
	err := reconnectLoop(testCtx(t), fastRetry, r.dial, play, noLog, noSleep)

	// Two lost connections, then the scripted clean close ends the session.
	if plays.Load() != 3 {
		t.Fatalf("play ran %d times, want 3 (two reconnects)", plays.Load())
	}
	if r.dials.Load() != 3 {
		t.Fatalf("dialled %d times, want 3 (first connect + two reconnects)", r.dials.Load())
	}
	if err == nil || !strings.Contains(err.Error(), "connection closed by server") {
		t.Fatalf("final error = %v, want the server's clean close", err)
	}
}

func TestKeepsRetryingWhileTheProxyIsDown(t *testing.T) {
	r := newLoopRig(t)
	var plays atomic.Int32

	// The connection drops and the next 3 dials are refused (the proxy pod is
	// being replaced); the 4th gets through.
	drop := errors.New("read: connection reset by peer")
	play := func(ctx context.Context, c *websocket.Conn) error {
		if plays.Load() == 0 {
			r.failDials.Store(3)
		}
		return scripted(&plays, drop)(ctx, c)
	}
	err := reconnectLoop(testCtx(t), fastRetry, r.dial, play, noLog, noSleep)

	if plays.Load() != 2 {
		t.Fatalf("play ran %d times, want 2", plays.Load())
	}
	if r.dials.Load() != 5 { // 1 first connect + 3 refused + 1 that worked
		t.Fatalf("dials = %d, want 5", r.dials.Load())
	}
	if err == nil || !strings.Contains(err.Error(), "connection closed by server") {
		t.Fatalf("final error = %v, want the server's clean close", err)
	}
}

func TestFirstDialFailureIsNotRetried(t *testing.T) {
	var dials atomic.Int32
	dial := func(context.Context) (*websocket.Conn, *http.Response, error) {
		dials.Add(1)
		return nil, nil, errors.New("dial tcp: connection refused")
	}
	err := reconnectLoop(testCtx(t), fastRetry, dial, nil, noLog, noSleep)
	if err == nil || !strings.Contains(err.Error(), "connect error") {
		t.Fatalf("err = %v, want connect error", err)
	}
	if dials.Load() != 1 {
		t.Fatalf("dialled %d times, want 1", dials.Load())
	}
}

func TestDoesNotReconnectAfterTheServerEndsTheSession(t *testing.T) {
	for _, code := range []websocket.StatusCode{websocket.StatusNormalClosure, websocket.StatusPolicyViolation, 4001} {
		r := newLoopRig(t)
		var plays atomic.Int32
		play := scripted(&plays, websocket.CloseError{Code: code})
		err := reconnectLoop(testCtx(t), fastRetry, r.dial, play, noLog, noSleep)
		if err == nil {
			t.Fatalf("code %d: no error", code)
		}
		if plays.Load() != 1 || r.dials.Load() != 1 {
			t.Errorf("code %d: plays=%d dials=%d, want 1 and 1 (no reconnect)", code, plays.Load(), r.dials.Load())
		}
	}
}

func TestStopsWhenReconnectIsRefused(t *testing.T) {
	r := newLoopRig(t)
	var plays atomic.Int32
	play := func(ctx context.Context, _ *websocket.Conn) error {
		plays.Add(1)
		r.refuse.Store(403) // the proxy now says forbidden
		return errors.New("read: connection reset by peer")
	}
	err := reconnectLoop(testCtx(t), fastRetry, r.dial, play, noLog, noSleep)
	if err == nil || !strings.Contains(err.Error(), "reconnect refused") {
		t.Fatalf("err = %v, want reconnect refused", err)
	}
	if plays.Load() != 1 {
		t.Fatalf("play ran %d times, want 1", plays.Load())
	}
}

func TestGivesUpAfterTheMaxOutage(t *testing.T) {
	r := newLoopRig(t)
	pol := retryPolicy{Initial: time.Millisecond, Max: 2 * time.Millisecond, MaxOutage: 50 * time.Millisecond}
	var plays atomic.Int32
	play := func(ctx context.Context, _ *websocket.Conn) error {
		plays.Add(1)
		r.failDials.Store(1 << 30) // from now on every dial fails
		return errors.New("read: connection reset by peer")
	}
	sleep := func(ctx context.Context, d time.Duration) error { return sleepCtx(ctx, d) }

	start := time.Now()
	err := reconnectLoop(testCtx(t), pol, r.dial, play, noLog, sleep)
	if !errors.Is(err, errGaveUp) {
		t.Fatalf("err = %v, want errGaveUp", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("took %v to give up", time.Since(start))
	}
}

func TestStopsWhenTheContextEnds(t *testing.T) {
	r := newLoopRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	play := func(ctx context.Context, _ *websocket.Conn) error {
		cancel() // the user quits while the connection is up
		return errors.New("read: use of closed network connection")
	}
	err := reconnectLoop(ctx, fastRetry, r.dial, play, noLog, sleepCtx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if r.dials.Load() != 1 {
		t.Fatalf("dialled %d times after quitting, want 1", r.dials.Load())
	}
}

func TestBackoffGrowsAndIsCapped(t *testing.T) {
	r := newLoopRig(t)
	pol := retryPolicy{Initial: 10 * time.Millisecond, Max: 40 * time.Millisecond, MaxOutage: time.Minute}
	var waits []time.Duration
	var plays atomic.Int32

	drop := errors.New("EOF")
	play := func(ctx context.Context, c *websocket.Conn) error {
		if plays.Load() == 0 {
			r.failDials.Store(5) // five refused dials in a row, then success
		}
		return scripted(&plays, drop)(ctx, c)
	}
	sleep := func(ctx context.Context, d time.Duration) error { waits = append(waits, d); return ctx.Err() }
	_ = reconnectLoop(testCtx(t), pol, r.dial, play, noLog, sleep)

	if len(waits) != 6 { // five refused dials + the successful one
		t.Fatalf("slept %d times (%v), want 6", len(waits), waits)
	}
	// Base waits are 10, 20, 40, 40, 40, 40 ms; jitter scales each by [0.5, 1.5).
	base := []time.Duration{10, 20, 40, 40, 40, 40}
	for i, b := range base {
		b *= time.Millisecond
		if waits[i] < b/2 || waits[i] >= b*3/2 {
			t.Errorf("wait %d = %v, want within [%v, %v)", i, waits[i], b/2, b*3/2)
		}
	}
}
