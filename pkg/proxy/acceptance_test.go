package proxy

// Acceptance tests: the promises the ingress proxy makes to the rest of the
// team, written to read top to bottom. They use real WebSockets over real
// local sockets, but no Kubernetes, Redis or game engine, so they run in a few
// seconds on any machine:
//
//	go test ./pkg/proxy -run Acceptance -v
//
// If one of these fails, the proxy is broken, not the game server.

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// player is one simulated client: it keeps sending a message every few
// milliseconds and records what comes back and whether its socket ever broke.
type player struct {
	id string
	cl *client

	mu       sync.Mutex
	fromA    int   // replies served by the old primary
	fromB    int   // replies served by the new primary
	aAfterB  int   // replies from the old primary AFTER the first one from the new one (must stay 0)
	sawB     bool  // has the player heard from the new primary yet
	droppedB error // first error on the player's socket, nil if it stayed connected
}

func (p *player) drop(err error) {
	p.mu.Lock()
	if p.droppedB == nil {
		p.droppedB = err
	}
	p.mu.Unlock()
}

func (p *player) counts() (a, b int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fromA, p.fromB
}

// run plays until stop is closed.
func (p *player) run(stop <-chan struct{}, wg *sync.WaitGroup) {
	wg.Add(2)
	go func() { // writer: one move every 10ms
		defer wg.Done()
		tick := time.NewTicker(10 * time.Millisecond)
		defer tick.Stop()
		for n := 0; ; n++ {
			select {
			case <-stop:
				return
			case <-tick.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := p.cl.c.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf("%s-move-%d", p.id, n)))
				cancel()
				if err != nil {
					p.drop(fmt.Errorf("write: %w", err))
					return
				}
			}
		}
	}()
	go func() { // reader: tallies who answered
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			case err := <-p.cl.errc:
				p.drop(fmt.Errorf("read: %w", err))
				return
			case m := <-p.cl.msgs:
				p.mu.Lock()
				switch {
				case strings.HasPrefix(m, "A|"):
					p.fromA++
					if p.sawB {
						p.aAfterB++
					}
				case strings.HasPrefix(m, "B|"):
					p.fromB++
					p.sawB = true
				}
				p.mu.Unlock()
			}
		}
	}()
}

// announce sends a takeover notice exactly the way the failure detector does
// (pkg/detector: POST /admin/route with a JSON body and a bearer token).
func announce(t *testing.T, p *Proxy, token, match string, epoch uint64, addr string) {
	t.Helper()
	body := fmt.Sprintf(`{"match_id":%q,"epoch":%d,"primary_addr":%q}`, match, epoch, addr)
	rec := adminDo(p.AdminHandler(), http.MethodPost, "/admin/route", body, token)
	if rec.Code != http.StatusOK {
		t.Fatalf("takeover notice rejected: HTTP %d %s", rec.Code, strings.TrimSpace(rec.Body.String()))
	}
}

// The headline promise: players keep playing through a primary failure, on
// the same WebSocket, without a single disconnect.
func TestAcceptance_PlayersSurviveAPrimaryCrash(t *testing.T) {
	playThroughTakeover(t, true)
}

// The same, when the old primary is still alive and talking while the new one
// is announced (a step-down, or a primary that is only partitioned away). The
// proxy must cut the old one off: once the new primary has answered, nothing
// more may reach the player from the old one.
func TestAcceptance_PlayersSurviveATakeoverWhileTheOldPrimaryIsStillAlive(t *testing.T) {
	playThroughTakeover(t, false)
}

func playThroughTakeover(t *testing.T, crash bool) {
	t.Helper()
	checkGoroutines(t)
	const (
		players = 20
		token   = "s3cret"
		match   = "match-000"
	)
	r := newRig(t, func(c *Config) { c.AdminToken = token })
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	announce(t, r.p, token, match, 1, a.Addr()) // A is the primary at epoch 1

	// 1. Twenty players join the same match through the proxy.
	var wg sync.WaitGroup
	stop := make(chan struct{})
	ps := make([]*player, players)
	for i := range ps {
		ps[i] = &player{id: fmt.Sprintf("p%d", i), cl: r.connect(t, match)}
		ps[i].run(stop, &wg)
	}
	eventually(t, "every player to get answers from A", func() bool {
		for _, p := range ps {
			if a, _ := p.counts(); a < 5 {
				return false
			}
		}
		return true
	})
	t.Logf("%d players are playing against A", players)

	// 2. The primary fails: crashes (connections cut without a goodbye), or
	//    simply stays alive and is replaced.
	if crash {
		a.Kill()
		t.Log("A crashed")
		time.Sleep(100 * time.Millisecond) // a short outage: nobody has told the proxy yet
	}

	// 3. The new primary is announced: epoch 2, B.
	announce(t, r.p, token, match, 2, b.Addr())
	t.Log("takeover announced: epoch 2 -> B")

	// 4. Everyone must be playing against B.
	eventually(t, "every player to get answers from B", func() bool {
		for _, p := range ps {
			if _, b := p.counts(); b < 5 {
				return false
			}
		}
		return true
	})
	if !crash {
		// A is still alive and still sending: give it the chance to leak.
		a.Send("A-speaking-after-takeover")
		time.Sleep(100 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	// 5. Verdict.
	for _, p := range ps {
		p.mu.Lock()
		if p.droppedB != nil {
			t.Errorf("%s was disconnected: %v", p.id, p.droppedB)
		}
		if p.aAfterB != 0 {
			t.Errorf("%s heard from the old primary %d times after the new one answered", p.id, p.aAfterB)
		}
		p.mu.Unlock()
	}
	if got := r.p.Stats().Retargets.Load(); got != players {
		t.Errorf("retargets = %d, want %d (one per player)", got, players)
	}
	if route, _ := r.p.Table().Lookup(match); route.Epoch != 2 || route.Addr != b.Addr() {
		t.Errorf("route = %+v, want epoch 2 at B", route)
	}
	t.Logf("PASS: %d players, 0 disconnects, all moved A -> B", players)
}

// The same promise when nobody sends a takeover notice: the proxy asks the
// pods for their role (GET /role) and follows the highest epoch, ignoring a
// fenced pod that still calls itself primary.
func TestAcceptance_PlayersSurviveWhenTheNoticeNeverArrives(t *testing.T) {
	checkGoroutines(t)
	const (
		players = 10
		match   = "match-000"
	)
	r := newRig(t, nil)
	a, b, z := newFakeGame(t, "A"), newFakeGame(t, "B"), newFakeGame(t, "Z")
	a.setRole("primary", 2, match)
	b.setRole("backup", 0, match)
	// Z is a fenced pod: it still believes it is primary, but with an older
	// epoch. Following it would be the split-brain bug seen on the cluster.
	z.setRole("primary", 1, match)

	d := NewDiscoverer(r.p, DiscoveryConfig{
		Targets:  StaticTargets(z.Addr(), a.Addr(), b.Addr()),
		Interval: 50 * time.Millisecond,
		Timeout:  500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	dDone := make(chan struct{})
	go func() { d.Run(ctx); close(dDone) }()
	t.Cleanup(func() { cancel(); <-dDone })

	var wg sync.WaitGroup
	stop := make(chan struct{})
	ps := make([]*player, players)
	for i := range ps {
		ps[i] = &player{id: fmt.Sprintf("p%d", i), cl: r.connect(t, match)} // no route exists yet
		ps[i].run(stop, &wg)
	}
	eventually(t, "every player to get answers from A", func() bool {
		for _, p := range ps {
			if a, _ := p.counts(); a < 5 {
				return false
			}
		}
		return true
	})

	a.Kill()
	time.Sleep(100 * time.Millisecond)
	b.setRole("primary", 3, match) // the standby promotes itself; no message goes to the proxy

	eventually(t, "every player to get answers from B", func() bool {
		for _, p := range ps {
			if _, b := p.counts(); b < 5 {
				return false
			}
		}
		return true
	})
	close(stop)
	wg.Wait()

	for _, p := range ps {
		p.mu.Lock()
		if p.droppedB != nil {
			t.Errorf("%s was disconnected: %v", p.id, p.droppedB)
		}
		if p.aAfterB != 0 {
			t.Errorf("%s heard from the old primary after the new one answered", p.id)
		}
		p.mu.Unlock()
	}
}

// The interface other people code against: what the failure detector may
// send to POST /admin/route and what it gets back.
func TestAcceptance_TakeoverNoticeContract(t *testing.T) {
	const token = "s3cret"
	p := New(Config{AdminToken: token})
	h := p.AdminHandler()
	post := func(tok, body string) (int, string) {
		rec := adminDo(h, http.MethodPost, "/admin/route", body, tok)
		return rec.Code, strings.TrimSpace(rec.Body.String())
	}
	const pod2 = "arcade-gs-b-0.arcade-gs.arcade.svc.cluster.local:8081"
	const pod1 = "arcade-gs-a-0.arcade-gs.arcade.svc.cluster.local:8081"
	notice := func(epoch int, addr string) string {
		return fmt.Sprintf(`{"match_id":"match-000","epoch":%d,"primary_addr":%q}`, epoch, addr)
	}

	for _, c := range []struct {
		name, token, body string
		wantCode          int
		wantResult        string
	}{
		{"first notice is applied", token, notice(1, pod1), 200, `"applied"`},
		{"a newer epoch replaces it", token, notice(2, pod2), 200, `"applied"`},
		{"resending the same notice is harmless", token, notice(2, pod2), 200, `"duplicate"`},
		{"a late notice with an older epoch is ignored", token, notice(1, pod1), 200, `"stale"`},
		{"two primaries for one epoch is reported", token, notice(2, pod1), 409, `"conflict"`},
		{"a wrong token is refused", "wrong", notice(3, pod1), 401, ""},
		{"no token is refused", "", notice(3, pod1), 401, ""},
		{"an invalid address is refused", token, notice(3, "not-an-address"), 400, ""},
		{"an invalid match id is refused", token, `{"match_id":"bad id!","epoch":3,"primary_addr":"h:1"}`, 400, ""},
		{"unknown fields are refused", token, `{"match_id":"match-000","epoch":3,"primary_addr":"h:1","extra":1}`, 400, ""},
		{"garbage is refused", token, `nope`, 400, ""},
	} {
		code, body := post(c.token, c.body)
		if code != c.wantCode || (c.wantResult != "" && !strings.Contains(body, c.wantResult)) {
			t.Errorf("%s: got HTTP %d %q, want HTTP %d %s", c.name, code, body, c.wantCode, c.wantResult)
		}
	}

	// After all that the route is the newest one that was legitimately applied.
	if route, ok := p.Table().Lookup("match-000"); !ok || route.Epoch != 2 || route.Addr != pod2 {
		t.Errorf("final route = %+v, %v; want epoch 2 at %s", route, ok, pod2)
	}
}
