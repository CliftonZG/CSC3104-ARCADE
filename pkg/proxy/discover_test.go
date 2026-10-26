package proxy

import (
	"context"
	"reflect"
	"testing"
	"time"
)

// newDiscoveryProxy returns a proxy and a Discoverer over the given pods. The
// poll interval is an hour: tests poll by hand or rely on Kick.
func newDiscoveryProxy(t *testing.T, pods ...*fakeGame) (*Proxy, *Discoverer) {
	t.Helper()
	p := New(Config{})
	addrs := make([]string, len(pods))
	for i, g := range pods {
		addrs[i] = g.Addr()
	}
	d := NewDiscoverer(p, DiscoveryConfig{
		Targets:  StaticTargets(addrs...),
		Interval: time.Hour,
		Timeout:  500 * time.Millisecond,
	})
	return p, d
}

func routeOf(t *testing.T, p *Proxy, match string) (Route, bool) {
	t.Helper()
	return p.Table().Lookup(match)
}

func TestDiscoveryPicksTheHighestEpochPrimary(t *testing.T) {
	// The situation from the first Kubernetes run: both pods say "primary",
	// but b was fenced (epoch 1) when a took over (epoch 2).
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("primary", 2, "m1")
	b.setRole("primary", 1, "m1")
	p, d := newDiscoveryProxy(t, a, b)

	u, ok := d.PollOnce(context.Background(), "m1")
	if !ok || u.PrimaryAddr != a.Addr() || u.Epoch != 2 {
		t.Fatalf("PollOnce = %+v, %v; want A at epoch 2", u, ok)
	}
	if r, _ := routeOf(t, p, "m1"); r.Addr != a.Addr() || r.Epoch != 2 {
		t.Fatalf("route = %+v, want A at epoch 2", r)
	}
	if again, ok := d.PollOnce(context.Background(), "m1"); ok {
		t.Fatalf("second poll changed the route: %+v", again)
	}
}

func TestDiscoveryIgnoresBackupsAndPodsThatAreNotReady(t *testing.T) {
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("backup", 0, "m1")
	b.setRole("primary", 1, "m1")
	b.notReady = true
	p, d := newDiscoveryProxy(t, a, b)

	if u, ok := d.PollOnce(context.Background(), "m1"); ok {
		t.Fatalf("applied %+v although nobody is a ready primary", u)
	}
	if _, ok := routeOf(t, p, "m1"); ok {
		t.Fatal("a route exists although nobody is a ready primary")
	}
}

func TestDiscoverySkipsPodsThatAreDownOrHaveNoRoleEndpoint(t *testing.T) {
	a, b, c := newFakeGame(t, "A"), newFakeGame(t, "B"), newFakeGame(t, "C")
	a.setRole("primary", 1, "m1")
	a.Kill() // crashed: connection refused
	// b has no /role endpoint at all (404)
	c.setRole("primary", 2, "m1")
	p, d := newDiscoveryProxy(t, a, b, c)

	if _, ok := d.PollOnce(context.Background(), "m1"); !ok {
		t.Fatal("no route found")
	}
	if r, _ := routeOf(t, p, "m1"); r.Addr != c.Addr() {
		t.Fatalf("route = %+v, want C", r)
	}
}

func TestDiscoveryNeverMovesClientsBackwards(t *testing.T) {
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("primary", 3, "m1")
	p, d := newDiscoveryProxy(t, a, b)

	// A takeover notice for epoch 5 arrived first.
	if res, err := p.ApplyUpdate(Update{MatchID: "m1", Epoch: 5, PrimaryAddr: b.Addr()}); err != nil || res != Applied {
		t.Fatalf("notice: %q, %v", res, err)
	}
	if u, ok := d.PollOnce(context.Background(), "m1"); ok {
		t.Fatalf("discovery overrode a newer route with %+v", u)
	}
	if r, _ := routeOf(t, p, "m1"); r.Addr != b.Addr() || r.Epoch != 5 {
		t.Fatalf("route = %+v, want B at epoch 5", r)
	}
}

func TestDiscoveryLeavesTheRouteAloneWhenTwoPodsClaimTheSameEpoch(t *testing.T) {
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("primary", 2, "m1")
	b.setRole("primary", 2, "m1")
	p, d := newDiscoveryProxy(t, a, b)

	if u, ok := d.PollOnce(context.Background(), "m1"); ok {
		t.Fatalf("applied %+v although two pods claim epoch 2", u)
	}
	if _, ok := routeOf(t, p, "m1"); ok {
		t.Fatal("a route was installed from an ambiguous answer")
	}
}

func TestDiscoveryIgnoresAPodThatServesAnotherMatch(t *testing.T) {
	a := newFakeGame(t, "A")
	a.setRole("primary", 1, "some-other-match")
	p, d := newDiscoveryProxy(t, a)

	if u, ok := d.PollOnce(context.Background(), "m1"); ok {
		t.Fatalf("applied %+v from a pod of another match", u)
	}
	if _, ok := routeOf(t, p, "m1"); ok {
		t.Fatal("route installed from the wrong match")
	}
}

func TestDiscoveryAgreesWithAnIdenticalTakeoverNotice(t *testing.T) {
	// The detector announces epoch 2 at address X; discovery then finds the
	// same epoch at the same address. That must be a quiet no-op, not a conflict.
	a := newFakeGame(t, "A")
	a.setRole("primary", 2, "m1")
	p, d := newDiscoveryProxy(t, a)

	if res, _ := p.ApplyUpdate(Update{MatchID: "m1", Epoch: 2, PrimaryAddr: a.Addr()}); res != Applied {
		t.Fatalf("notice: %q", res)
	}
	before := p.Stats().RoutesApplied.Load()
	if _, ok := d.PollOnce(context.Background(), "m1"); ok {
		t.Fatal("a duplicate was reported as applied")
	}
	if p.Stats().RoutesApplied.Load() != before {
		t.Fatal("the duplicate was counted as a new route")
	}
}

func TestDiscoveryFindsThePrimaryForAClientThatHasNoRoute(t *testing.T) {
	checkGoroutines(t)
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("backup", 0, "m1")
	b.setRole("primary", 1, "m1")
	d := NewDiscoverer(r.p, DiscoveryConfig{
		Targets:  StaticTargets(a.Addr(), b.Addr()),
		Interval: time.Hour, // only the nudge from the waiting client can trigger the poll
		Timeout:  500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	cl := r.connect(t, "m1") // no route exists
	cl.send(t, "hello")
	cl.expect(t, "B|hello")
	if a.numReqs() != 0 {
		t.Fatalf("the backup received %d client handshakes", a.numReqs())
	}
}

func TestDiscoveryMovesAClientWhenItsPrimaryDies(t *testing.T) {
	// No takeover notice is ever sent. The primary crashes, the standby is
	// promoted, and the proxy finds out only by asking.
	checkGoroutines(t)
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("primary", 1, "m1")
	b.setRole("backup", 0, "m1")
	d := NewDiscoverer(r.p, DiscoveryConfig{
		Targets:  StaticTargets(a.Addr(), b.Addr()),
		Interval: 50 * time.Millisecond,
		Timeout:  500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	cl := r.connect(t, "m1")
	cl.send(t, "one")
	cl.expect(t, "A|one")

	a.Kill()
	// Wait until the proxy knows A is gone, so the next message is queued
	// instead of being written to a dead socket (the in-flight loss that no
	// proxy can prevent).
	eventually(t, "the proxy to notice A is gone", func() bool { return r.p.Stats().DialFailures.Load() >= 1 })
	cl.send(t, "while-down") // buffered until a primary exists
	b.setRole("primary", 2, "m1")

	cl.expect(t, "B|while-down")
	cl.send(t, "after")
	cl.expect(t, "B|after")
	cl.expectQuiet(t, 100*time.Millisecond) // the client socket never saw an error or a close
	if got := r.p.Stats().Retargets.Load(); got != 1 {
		t.Fatalf("retargets = %d, want 1", got)
	}
}

func TestDiscoveryRepairsAFencedPodBeingTheRoute(t *testing.T) {
	// The bug found on the first cluster run: the proxy was told that the
	// promoted standby (epoch 1) is the primary, then the other pod claimed
	// epoch 2 by starting up and nobody told the proxy.
	checkGoroutines(t)
	r := newRig(t, nil)
	a, b := newFakeGame(t, "A"), newFakeGame(t, "B")
	a.setRole("primary", 2, "m1")
	b.setRole("primary", 1, "m1") // fenced, but still thinks it is primary
	r.route(t, "m1", 1, b)

	cl := r.connect(t, "m1")
	cl.send(t, "x")
	cl.expect(t, "B|x") // the client is on the fenced pod, as in the cluster

	d := NewDiscoverer(r.p, DiscoveryConfig{
		Targets:  StaticTargets(a.Addr(), b.Addr()),
		Interval: 50 * time.Millisecond,
		Timeout:  500 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { d.Run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })

	eventually(t, "the client to be moved to A", func() bool { return r.p.Stats().Retargets.Load() == 1 })
	cl.send(t, "y")
	cl.expect(t, "A|y")
	cl.expectQuiet(t, 100*time.Millisecond)
}

func TestOnlyMatchesWithClientsArePolled(t *testing.T) {
	r := newRig(t, nil)
	a := newFakeGame(t, "A")
	a.setRole("primary", 1, "m1")
	d := NewDiscoverer(r.p, DiscoveryConfig{Targets: StaticTargets(a.Addr()), Interval: time.Hour})

	d.pollAll(context.Background())
	if _, ok := r.p.Table().Lookup("m1"); ok {
		t.Fatal("a match without clients was polled")
	}
	r.route(t, "m2", 1, a) // a route alone does not make a match active either
	if got := r.p.activeMatches(); len(got) != 0 {
		t.Fatalf("active matches = %v, want none", got)
	}

	cl := r.connect(t, "m1")
	_ = cl
	eventually(t, "m1 to be active", func() bool { return len(r.p.activeMatches()) == 1 })
	d.pollAll(context.Background())
	if rt, ok := r.p.Table().Lookup("m1"); !ok || rt.Epoch != 1 {
		t.Fatalf("route = %+v, %v; want epoch 1", rt, ok)
	}
}

func TestPodTargets(t *testing.T) {
	f := PodTargets("match-", []string{"arcade-gs-a", "arcade-gs-b"}, "arcade-gs", "arcade", "cluster.local", 8081)

	got := f("match-007")
	want := []string{
		"arcade-gs-a-7.arcade-gs.arcade.svc.cluster.local:8081",
		"arcade-gs-b-7.arcade-gs.arcade.svc.cluster.local:8081",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("match-007 -> %v, want %v", got, want)
	}
	for _, id := range []string{"room7", "match-", "match-x", "match-1x", "match--1", "Match-1", ""} {
		if got := f(id); len(got) != 0 {
			t.Errorf("%q -> %v, want no targets", id, got)
		}
	}
}
