package detector

// Evaluation harness for the report (Section IV): failure-detection latency and
// false takeovers of the dual-validation detector. It runs real primary and
// standby detectors over loopback UDP, a real game engine that commits every
// move through the Lua-fenced Redis pipeline (miniredis), and a lossy UDP
// relay that drops a chosen share of the primary's heartbeats.
//
// Skipped by default because it takes a few minutes:
//
//	ARCADE_EVAL=1 go test ./pkg/detector -run TestEval -v -timeout 30m
//
// Set ARCADE_EVAL_OUT=<dir> to also write the results as CSV files.

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/loadgen"
	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// evalConfig is one detector setting. Timeout and silence keep the default
// ratios (50 ms interval: 200 ms heartbeat timeout, 300 ms snapshot silence).
type evalConfig struct {
	Name     string
	Interval time.Duration
	Timeout  time.Duration
	Silence  time.Duration
}

var evalConfigs = []evalConfig{
	{"20ms", 20 * time.Millisecond, 80 * time.Millisecond, 120 * time.Millisecond},
	{"50ms", 50 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond},
}

func requireEval(t *testing.T) {
	t.Helper()
	if os.Getenv("ARCADE_EVAL") == "" {
		t.Skip("set ARCADE_EVAL=1 to run the detector evaluation")
	}
}

// evalPair is one primary/standby pair sharing a miniredis store.
type evalPair struct {
	store      *replication.Store
	statusB    *fleet.Status
	detA, detB *Detector
	relay      *lossyRelay
	stopPlay   context.CancelFunc
	playDone   chan struct{}
	commits    atomic.Int64
}

// lossyRelay forwards UDP datagrams to dst, dropping each with the current
// loss probability (stored in per-mille so it can change while running).
type lossyRelay struct {
	conn    *net.UDPConn
	dst     *net.UDPAddr
	permill atomic.Int64
	wg      sync.WaitGroup
}

func (r *lossyRelay) SetLoss(loss float64) { r.permill.Store(int64(loss * 1000)) }

func newLossyRelay(t *testing.T, dstPort int) (*lossyRelay, int) {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	r := &lossyRelay{conn: conn, dst: &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: dstPort}}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		rng := rand.New(rand.NewSource(time.Now().UnixNano()))
		buf := make([]byte, 2048)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if rng.Int63n(1000) < r.permill.Load() {
				continue
			}
			_, _ = conn.WriteToUDP(buf[:n], r.dst)
		}
	}()
	return r, conn.LocalAddr().(*net.UDPAddr).Port
}

func (r *lossyRelay) Close() { _ = r.conn.Close(); r.wg.Wait() }

func evalPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// startEvalPair starts a primary that plays one move per moveEvery, committing
// each through the replication hook, and a standby detector. The primary's
// heartbeats go through a relay (p.relay) whose loss starts at 0.
func startEvalPair(t *testing.T, ctx context.Context, cfg evalConfig, moveEvery time.Duration) *evalPair {
	t.Helper()
	quiet := func(string, ...any) {}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	store := replication.NewStore(rdb)
	matchID := "eval-match"

	portA, portB := evalPort(t), evalPort(t)
	relay, relayPort := newLossyRelay(t, portB)

	idA := fleet.Identity{PodName: "arcade-gs-a-0", MatchID: matchID, InitialRole: fleet.RolePrimary,
		HeartbeatPort: portA, PeerHeartbeatPort: relayPort, PeerHost: "127.0.0.1"}
	idB := fleet.Identity{PodName: "arcade-gs-b-0", MatchID: matchID, InitialRole: fleet.RoleBackup,
		HeartbeatPort: portB, PeerHeartbeatPort: portA, PeerHost: "127.0.0.1"}
	statusA := fleet.NewStatus(idA, fleet.RolePrimary)
	statusB := fleet.NewStatus(idB, fleet.RoleBackup)

	epoch, err := store.ClaimEpoch(ctx, matchID, 0)
	if err != nil {
		t.Fatal(err)
	}
	statusA.SetEpoch(epoch)

	game, err := uno.NewGameEngine(matchID, []string{"p1", "p2", "p3", "p4"}, 7)
	if err != nil {
		t.Fatal(err)
	}
	p := &evalPair{store: store, statusB: statusB, relay: relay, playDone: make(chan struct{})}
	rep := replication.NewReplicator(store, replication.ReplicatorConfig{MatchID: matchID, Epoch: epoch})
	game.RegisterHook(rep.Hook())
	game.RegisterHook(func(_, _ *uno.State, _ uno.ActionSummary) error { p.commits.Add(1); return nil })
	if err := rep.Publish(ctx, game.State()); err != nil {
		t.Fatal(err)
	}

	p.detA = New(Config{Identity: idA, Status: statusA, Store: store, Logf: quiet,
		HeartbeatInterval: cfg.Interval, HeartbeatTimeout: cfg.Timeout, SnapshotSilenceTimeout: cfg.Silence})
	p.detB = New(Config{Identity: idB, Status: statusB, Store: store, Logf: quiet,
		HeartbeatInterval: cfg.Interval, HeartbeatTimeout: cfg.Timeout, SnapshotSilenceTimeout: cfg.Silence})
	if err := p.detA.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.detB.Start(ctx); err != nil {
		t.Fatal(err)
	}

	playCtx, stop := context.WithCancel(ctx)
	p.stopPlay = stop
	go func() {
		defer close(p.playDone)
		tick := time.NewTicker(moveEvery)
		defer tick.Stop()
		for {
			select {
			case <-playCtx.Done():
				return
			case <-tick.C:
				if _, err := loadgen.Step(game, loadgen.Policy{Endless: true}); err != nil &&
					!errors.Is(err, uno.ErrHookFailed) {
					return
				}
			}
		}
	}()
	return p
}

// crash stops the primary's heartbeats and its moves at the same instant.
func (p *evalPair) crash() {
	p.stopPlay()
	p.detA.Stop()
	<-p.playDone
}

func (p *evalPair) close() {
	p.stopPlay()
	<-p.playDone
	p.detA.Stop()
	p.detB.Stop()
	p.relay.Close()
}

func percentile(sorted []time.Duration, q float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(q*float64(len(sorted)-1) + 0.5)
	return sorted[i]
}

func ms(d time.Duration) string { return fmt.Sprintf("%.1f", float64(d.Microseconds())/1000) }

func writeEvalCSV(t *testing.T, name string, rows []string) {
	dir := os.Getenv("ARCADE_EVAL_OUT")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(strings.Join(rows, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", path)
}

// TestEvalDetectionLatency: crash the primary N times per setting and measure
// the time until the standby has claimed the next epoch and serves the match.
func TestEvalDetectionLatency(t *testing.T) {
	requireEval(t)
	const trials = 30
	moveEvery := 200 * time.Millisecond // 5 moves/s, as in the load test
	rows := []string{"config,trial,detection_ms"}
	summary := []string{"config,trials,median_ms,p95_ms,max_ms"}

	for _, cfg := range evalConfigs {
		var lat []time.Duration
		for i := 1; i <= trials; i++ {
			ctx, cancel := context.WithCancel(context.Background())
			p := startEvalPair(t, ctx, cfg, moveEvery)
			time.Sleep(time.Second) // warm up: heartbeats and a few commits
			// Crash at a random point between two moves.
			time.Sleep(time.Duration(rand.Int63n(int64(moveEvery))))
			p.crash()
			start := time.Now()
			for !p.statusB.IsPrimary() && time.Since(start) < 5*time.Second {
				time.Sleep(time.Millisecond)
			}
			d := time.Since(start)
			if !p.statusB.IsPrimary() {
				t.Errorf("%s trial %d: standby never took over", cfg.Name, i)
			} else {
				lat = append(lat, d)
				rows = append(rows, fmt.Sprintf("%s,%d,%s", cfg.Name, i, ms(d)))
			}
			p.close()
			cancel()
		}
		sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
		line := fmt.Sprintf("%s,%d,%s,%s,%s", cfg.Name, len(lat),
			ms(percentile(lat, 0.5)), ms(percentile(lat, 0.95)), ms(percentile(lat, 1)))
		summary = append(summary, line)
		t.Logf("[latency] interval=%s timeout=%s silence=%s: median %s ms, p95 %s ms, max %s ms (n=%d)",
			cfg.Interval, cfg.Timeout, cfg.Silence,
			ms(percentile(lat, 0.5)), ms(percentile(lat, 0.95)), ms(percentile(lat, 1)), len(lat))
	}
	writeEvalCSV(t, "detector-latency-trials.csv", rows)
	writeEvalCSV(t, "detector-latency-summary.csv", summary)
}

// TestEvalFalsePositives: keep the primary healthy, drop a share of its
// heartbeats, and count (a) heartbeat flatlines, i.e. how often a
// heartbeat-only detector would have promoted, and (b) actual takeovers by
// the dual-validation detector. Repeated for a fast and a slow move rate,
// because the snapshot-silence check only helps while moves keep arriving.
func TestEvalFalsePositives(t *testing.T) {
	requireEval(t)
	window := 20 * time.Second
	if v := os.Getenv("ARCADE_EVAL_WINDOW"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			window = d
		}
	}
	losses := []float64{0, 0.10, 0.30, 1.0}
	moveRates := []time.Duration{200 * time.Millisecond, 2 * time.Second}
	rows := []string{"config,move_every_ms,loss_pct,window_s,flatlines,takeovers,first_takeover_s"}

	for _, cfg := range evalConfigs {
		for _, moveEvery := range moveRates {
			for _, loss := range losses {
				ctx, cancel := context.WithCancel(context.Background())
				p := startEvalPair(t, ctx, cfg, moveEvery)
				// Healthy warm-up first, so the standby has seen the primary's
				// epoch (as in a real deployment), then start dropping heartbeats.
				time.Sleep(time.Second + moveEvery)
				p.relay.SetLoss(loss)
				start := time.Now()
				flatlines, inFlatline := 0, false
				firstTakeover := time.Duration(-1)
				tick := time.NewTicker(cfg.Interval / 2)
				for time.Since(start) < window {
					<-tick.C
					flat := time.Since(time.Unix(0, p.detB.lastPeerHb.Load())) > cfg.Timeout
					if flat && !inFlatline {
						flatlines++
					}
					inFlatline = flat
					if p.statusB.IsPrimary() {
						firstTakeover = time.Since(start)
						break // a false takeover ends the run: the healthy primary is now fenced
					}
				}
				tick.Stop()
				takeovers := 0
				first := "-"
				if firstTakeover >= 0 {
					takeovers = 1
					first = fmt.Sprintf("%.1f", firstTakeover.Seconds())
				}
				rows = append(rows, fmt.Sprintf("%s,%d,%.0f,%.0f,%d,%d,%s",
					cfg.Name, moveEvery.Milliseconds(), loss*100, window.Seconds(), flatlines, takeovers, first))
				t.Logf("[false-positive] interval=%s moves every %s loss=%.0f%%: %d heartbeat flatlines, false takeover: %s",
					cfg.Interval, moveEvery, loss*100, flatlines, map[bool]string{true: "YES after " + first + " s", false: "no"}[takeovers > 0])
				p.close()
				cancel()
			}
		}
	}
	writeEvalCSV(t, "detector-false-positives.csv", rows)
}
