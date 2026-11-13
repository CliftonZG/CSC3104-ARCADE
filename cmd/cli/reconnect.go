package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"time"

	"github.com/coder/websocket"
)

// retryPolicy controls how the remote client re-dials after its WebSocket to
// the proxy (or gameserver) is lost, for example because the proxy pod it was
// connected to crashed or was drained.
//
// The server sends the current match state as soon as a socket is accepted
// (cmd/gameserver/ws.go: sendCurrentState), and the match state itself lives
// in Redis, so a client that dials the same URL again simply rejoins the game.
type retryPolicy struct {
	Initial   time.Duration // first wait after a lost connection
	Max       time.Duration // the wait grows (x2) up to this
	MaxOutage time.Duration // give up when no connection could be made for this long
}

var defaultRetry = retryPolicy{
	Initial:   250 * time.Millisecond,
	Max:       3 * time.Second,
	MaxOutage: 60 * time.Second,
}

// errGaveUp is returned when the outage lasted longer than retryPolicy.MaxOutage.
var errGaveUp = errors.New("could not reconnect in time")

// shouldRetry reports whether a lost or failed connection is worth another try.
//
// Terminal answers mirror what the proxy forwards (docs/proxy.md): a clean
// close, a policy violation (rejected handshake or protocol error), a too big
// message, and the application range 3000-4999 mean "this player's session is
// over", so reconnecting would only repeat the same refusal. Everything else is
// treated as an outage: abnormal drops (1006), going away (1001, a proxy that
// is draining), service restart (1012), try again later (1013) and bad
// gateway (1014), and plain network errors.
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	switch code := websocket.CloseStatus(err); {
	case code == -1:
		return true // not a close frame: network error, EOF, reset
	case code == websocket.StatusNormalClosure,
		code == websocket.StatusPolicyViolation,
		code == websocket.StatusMessageTooBig:
		return false
	case code >= 3000 && code <= 4999:
		return false
	default:
		return true
	}
}

// handshakeTerminal reports whether a failed WebSocket handshake will keep
// failing no matter how often it is retried (bad request, not authenticated,
// forbidden, unknown route). 5xx answers and no answer at all are outages.
func handshakeTerminal(resp *http.Response) bool {
	if resp == nil {
		return false
	}
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound:
		return true
	}
	return false
}

// dialFunc opens a connection. It returns the HTTP response of a failed
// handshake, if any, so the caller can tell a refusal from an outage.
type dialFunc func(ctx context.Context) (*websocket.Conn, *http.Response, error)

// sessionFunc plays on one connection until it breaks. It returns the error
// that ended the connection.
type sessionFunc func(ctx context.Context, conn *websocket.Conn) error

// reconnectLoop dials, plays, and dials again whenever the connection is lost
// for a reason shouldRetry accepts, until ctx ends, the server ends the session
// for good, or no connection could be made for pol.MaxOutage.
//
// The very first dial is not retried: a wrong URL or a proxy that is not there
// should fail at once, as it did before reconnecting existed.
//
// sleep and logf are injected so tests need neither real time nor a terminal.
func reconnectLoop(
	ctx context.Context,
	pol retryPolicy,
	dial dialFunc,
	play sessionFunc,
	logf func(format string, args ...any),
	sleep func(context.Context, time.Duration) error,
) error {
	conn, _, err := dial(ctx)
	if err != nil {
		return fmt.Errorf("connect error: %w", err)
	}

	for {
		err = play(ctx, conn)
		_ = conn.CloseNow()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !shouldRetry(err) {
			return fmt.Errorf("connection closed by server: %w", err)
		}

		logf("connection lost (%v), reconnecting...", err)
		lostAt := time.Now()
		wait := pol.Initial

		for {
			if time.Since(lostAt) > pol.MaxOutage {
				return fmt.Errorf("%w (%v): %w", errGaveUp, pol.MaxOutage, err)
			}
			if serr := sleep(ctx, jitter(wait)); serr != nil {
				return serr
			}
			var resp *http.Response
			conn, resp, err = dial(ctx)
			if err == nil {
				logf("reconnected after %v", time.Since(lostAt).Round(time.Millisecond))
				break
			}
			if handshakeTerminal(resp) || !shouldRetry(err) {
				return fmt.Errorf("reconnect refused: %w", err)
			}
			wait = min(wait*2, pol.Max)
		}
	}
}

// jitter spreads retries of many clients that lost the same proxy at the same
// moment, so they do not all return in one burst. It returns d scaled by a
// random factor in [0.5, 1.5).
func jitter(d time.Duration) time.Duration {
	return time.Duration(float64(d) * (0.5 + rand.Float64()))
}

// sleepCtx waits for d or until ctx ends.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
