// Package loadgen drives synthetic Uno traffic through a real game engine so
// the fleet can be load-tested and profiled before (and independently of)
// the WebSocket proxy.
//
// Owner: Tan Yan Ting (Phase 2: horizontal scalability & resource profiling).
//
// Each move goes through Game.ApplyAction exactly like a client move would,
// so every registered MoveHook runs: the Redis replication hook (snapshot +
// Lua-fenced commit), the metrics hook, and anything added later. The cost
// measured under load is therefore the real per-move cost of a primary pod.
package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	applyLatency = promauto.NewHistogram(prometheus.HistogramOpts{
		Namespace: "arcade",
		Subsystem: "loadgen",
		Name:      "apply_latency_seconds",
		Help:      "End-to-end Game.ApplyAction latency for synthetic moves, including all move hooks (Redis commit).",
		Buckets:   []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5},
	})
	movesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "arcade",
		Subsystem: "loadgen",
		Name:      "moves_total",
		Help:      "Synthetic moves attempted, by outcome (ok, rejected, hook_error).",
	}, []string{"outcome"})
)

// Policy chooses moves for the self-play bot.
type Policy struct {
	// Endless keeps the match running forever: a player never plays their
	// last two cards (they draw and pass instead), so nobody wins. This keeps
	// the per-pod workload stationary for long profiling windows.
	Endless bool
	Rand    *rand.Rand
}

// Candidates returns the legal-looking actions for the current player in
// preference order. The engine is the judge: the driver applies them in
// order until one succeeds, so a stale or imperfect suggestion is harmless.
func (p Policy) Candidates(st *uno.State) []uno.Action {
	if st == nil || st.Status == uno.StatusFinished {
		return nil
	}
	player := st.CurrentPlayer()
	hand := st.Hands[player]

	var playable []int
	for i, c := range hand {
		if uno.IsPlayable(st, player, c) {
			playable = append(playable, i)
		}
	}
	if p.Rand != nil {
		p.Rand.Shuffle(len(playable), func(i, j int) { playable[i], playable[j] = playable[j], playable[i] })
	}

	plays := make([]uno.Action, 0, len(playable))
	for _, i := range playable {
		a := uno.Action{Type: uno.ActionPlay, PlayerID: player, CardIndex: i}
		if hand[i].IsWild() {
			a.DeclaredColour = dominantColour(hand)
		}
		plays = append(plays, a)
	}
	draw := uno.Action{Type: uno.ActionDraw, PlayerID: player}
	pass := uno.Action{Type: uno.ActionPass, PlayerID: player}

	hoard := p.Endless && len(hand) <= 2
	var out []uno.Action
	switch {
	case st.DrewThisTurn && hoard:
		out = append(out, pass)
		out = append(out, plays...)
	case st.DrewThisTurn:
		out = append(out, plays...)
		out = append(out, pass)
	case hoard:
		out = append(out, draw)
		out = append(out, plays...)
	default:
		out = append(out, plays...)
		out = append(out, draw)
	}
	return out
}

// dominantColour picks the colour the player holds most of (for wilds).
func dominantColour(hand []uno.Card) uno.Colour {
	counts := map[uno.Colour]int{}
	for _, c := range hand {
		if uno.IsPlayableColour(c.Colour) {
			counts[c.Colour]++
		}
	}
	best, bestN := uno.ColourRed, -1
	for _, c := range uno.Colours() { // fixed order keeps it deterministic
		if counts[c] > bestN {
			best, bestN = c, counts[c]
		}
	}
	return best
}

// ErrStuck is returned when no candidate move is accepted by the engine.
var ErrStuck = errors.New("loadgen: no legal move accepted by the engine")

// Step plays one move: it applies the policy's candidates in order until the
// engine accepts one. A hook error (e.g. the Redis commit was fenced) is
// returned immediately: the move was applied in memory but not replicated,
// so the caller must stop driving this engine.
func Step(g *uno.Game, p Policy) (uno.Action, error) {
	st := g.State()
	if st.Status == uno.StatusFinished {
		return uno.Action{}, uno.ErrGameOver
	}
	var lastErr error
	for _, a := range p.Candidates(st) {
		start := time.Now()
		_, err := g.ApplyAction(a)
		switch {
		case err == nil:
			applyLatency.Observe(time.Since(start).Seconds())
			movesTotal.WithLabelValues("ok").Inc()
			return a, nil
		case errors.Is(err, uno.ErrHookFailed):
			applyLatency.Observe(time.Since(start).Seconds())
			movesTotal.WithLabelValues("hook_error").Inc()
			return a, err
		default:
			movesTotal.WithLabelValues("rejected").Inc()
			lastErr = err
		}
	}
	return uno.Action{}, fmt.Errorf("%w (last error: %v)", ErrStuck, lastErr)
}

// Driver plays a match on a fixed tick until ctx ends, the match finishes,
// or a hook fails.
type Driver struct {
	Game     *uno.Game
	Interval time.Duration // time between moves (per match)
	Policy   Policy
	Logf     func(string, ...any)
}

// Run blocks until the driver stops and returns why.
func (d *Driver) Run(ctx context.Context) error {
	if d.Interval <= 0 {
		return errors.New("loadgen: interval must be positive")
	}
	logf := d.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}
	// Spread start times so 100 pods started together do not tick in lockstep.
	jitter := time.Duration(rand.Int63n(int64(d.Interval)))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(jitter):
	}

	ticker := time.NewTicker(d.Interval)
	defer ticker.Stop()
	moves := 0
	for {
		_, err := Step(d.Game, d.Policy)
		switch {
		case err == nil:
			moves++
		case errors.Is(err, uno.ErrGameOver):
			logf("[LOADGEN] match %s finished after %d moves (winner %s)", d.Game.MatchID(), moves, d.Game.Winner())
			return nil
		case errors.Is(err, ErrStuck):
			logf("[LOADGEN] %v; stopping", err)
			return err
		default:
			logf("[LOADGEN] move hook failed after %d moves, stopping driver: %v", moves, err)
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
