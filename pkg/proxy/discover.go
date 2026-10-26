package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RolePath is the path a game pod serves its live role on.
const RolePath = "/role"

// maxRoleBody bounds how much of a /role response is read.
const maxRoleBody = 64 << 10

// roleInfo is the part of a game pod's GET /role document the proxy uses.
// The pod serves more fields (pod name, since, peer...); they are ignored.
type roleInfo struct {
	MatchID string `json:"match_id"`
	Role    string `json:"role"`
	Epoch   int64  `json:"epoch"`
	Ready   bool   `json:"ready"`
}

// DiscoveryConfig configures a Discoverer.
type DiscoveryConfig struct {
	// Targets returns the host:port of every pod that may be the primary of
	// the match. The same string becomes the route's primary_addr, so it must
	// be spelled the way the control plane spells it (otherwise a takeover
	// notice and a discovery result for the same epoch would look like a
	// conflict). Returning nothing disables discovery for that match.
	Targets func(matchID string) []string

	// Interval is how often every match with a connected client is polled
	// (default 1s). A client that has no route, or whose primary just failed,
	// triggers an immediate poll of its match without waiting for the tick.
	Interval time.Duration

	// Timeout bounds one GET /role (default 800ms).
	Timeout time.Duration

	// MaxParallel bounds the polls in flight at once (default 16).
	MaxParallel int
}

func (c *DiscoveryConfig) applyDefaults() {
	if c.Interval <= 0 {
		c.Interval = time.Second
	}
	if c.Timeout <= 0 {
		c.Timeout = 800 * time.Millisecond
	}
	if c.MaxParallel <= 0 {
		c.MaxParallel = 16
	}
}

// Discoverer finds the primary of each match by asking the candidate pods for
// their role (GET /role), and feeds the answer into the route table.
//
// It is a second, independent source of routes next to the takeover
// notifications (POST /admin/route). Both go through the same rule, a route
// is only replaced by a strictly higher epoch, so they can run together: a
// notification that is lost, late or never sent (for example a pod that
// became primary while starting up) is repaired by the next poll, and a poll
// can never move clients backwards.
//
// Among the pods that report role "primary", the highest epoch wins. That is
// what makes a fenced pod, which still believes it is primary but holds an
// older epoch, harmless.
type Discoverer struct {
	p    *Proxy
	cfg  DiscoveryConfig
	http *http.Client

	kick chan string

	mu       sync.Mutex
	lastPoll map[string]time.Time
}

// NewDiscoverer creates a Discoverer for p and registers it to be nudged
// whenever one of p's clients has no usable primary. Call Run to start it.
func NewDiscoverer(p *Proxy, cfg DiscoveryConfig) *Discoverer {
	cfg.applyDefaults()
	d := &Discoverer{
		p:   p,
		cfg: cfg,
		http: &http.Client{
			Transport:     p.httpClient.Transport,
			Timeout:       cfg.Timeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		kick:     make(chan string, 64),
		lastPoll: make(map[string]time.Time),
	}
	p.setRouteHint(d.Kick)
	return d
}

// Kick asks for an immediate poll of one match. It never blocks.
func (d *Discoverer) Kick(matchID string) {
	select {
	case d.kick <- matchID:
	default: // a full queue means polls are already pending
	}
}

// Run polls until ctx ends.
func (d *Discoverer) Run(ctx context.Context) {
	tick := time.NewTicker(d.cfg.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			d.pollAll(ctx)
		case m := <-d.kick:
			// A burst of clients of one match kicks once per client; poll the
			// match at most every 100ms.
			d.mu.Lock()
			recent := time.Since(d.lastPoll[m]) < 100*time.Millisecond
			d.mu.Unlock()
			if !recent {
				d.PollOnce(ctx, m)
			}
		}
	}
}

func (d *Discoverer) pollAll(ctx context.Context) {
	matches := d.p.activeMatches()
	sem := make(chan struct{}, d.cfg.MaxParallel)
	var wg sync.WaitGroup
	for _, m := range matches {
		sem <- struct{}{}
		wg.Add(1)
		go func(m string) {
			defer func() { <-sem; wg.Done() }()
			d.PollOnce(ctx, m)
		}(m)
	}
	wg.Wait()
	d.forgetPolls(matches)
}

// forgetPolls drops rate-limit state for matches that no longer have clients.
func (d *Discoverer) forgetPolls(active []string) {
	keep := make(map[string]struct{}, len(active))
	for _, m := range active {
		keep[m] = struct{}{}
	}
	d.mu.Lock()
	for m := range d.lastPoll {
		if _, ok := keep[m]; !ok {
			delete(d.lastPoll, m)
		}
	}
	d.mu.Unlock()
}

// PollOnce asks every candidate pod of the match for its role and applies the
// best answer to the route table. It reports the update it applied, if any.
func (d *Discoverer) PollOnce(ctx context.Context, matchID string) (Update, bool) {
	d.mu.Lock()
	d.lastPoll[matchID] = time.Now()
	d.mu.Unlock()

	addrs := d.cfg.Targets(matchID)
	if len(addrs) == 0 {
		return Update{}, false
	}

	type answer struct {
		addr string
		info roleInfo
		err  error
	}
	answers := make([]answer, len(addrs))
	var wg sync.WaitGroup
	for i, addr := range addrs {
		wg.Add(1)
		go func(i int, addr string) {
			defer wg.Done()
			info, err := d.fetchRole(ctx, addr)
			answers[i] = answer{addr, info, err}
		}(i, addr)
	}
	wg.Wait()

	var best *Update
	tied := false
	for _, a := range answers {
		if a.err != nil {
			d.p.log.Debug("role probe failed", "match", matchID, "addr", a.addr, "err", a.err)
			continue
		}
		if a.info.Role != "primary" || !a.info.Ready || a.info.Epoch < 0 {
			continue
		}
		if a.info.MatchID != "" && a.info.MatchID != matchID {
			d.p.log.Warn("pod serves a different match than expected; ignoring it",
				"addr", a.addr, "expected", matchID, "got", a.info.MatchID)
			continue
		}
		u := Update{MatchID: matchID, Epoch: uint64(a.info.Epoch), PrimaryAddr: a.addr}
		switch {
		case best == nil || u.Epoch > best.Epoch:
			best, tied = &u, false
		case u.Epoch == best.Epoch && u.PrimaryAddr != best.PrimaryAddr:
			tied = true
		}
	}
	if best == nil {
		return Update{}, false
	}
	if tied {
		d.p.log.Warn("two pods claim to be primary at the same epoch; not touching the route",
			"match", matchID, "epoch", best.Epoch)
		return Update{}, false
	}

	res, err := d.p.table.Apply(*best)
	if err != nil {
		d.p.log.Warn("discovered an invalid route", "match", matchID, "err", err)
		return Update{}, false
	}
	switch res {
	case Applied:
		d.p.stats.RoutesApplied.Add(1)
		d.p.log.Info("route updated", "match", matchID, "epoch", best.Epoch, "primary", best.PrimaryAddr, "source", "discovery")
		return *best, true
	case Conflict:
		d.p.log.Warn("discovery disagrees with the route table at the same epoch",
			"match", matchID, "epoch", best.Epoch, "primary", best.PrimaryAddr)
	}
	return Update{}, false // stale and duplicate answers are the normal case
}

func (d *Discoverer) fetchRole(ctx context.Context, addr string) (roleInfo, error) {
	var info roleInfo
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+RolePath, nil)
	if err != nil {
		return info, err
	}
	resp, err := d.http.Do(req)
	if err != nil {
		return info, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return info, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxRoleBody)).Decode(&info); err != nil {
		return info, fmt.Errorf("bad /role document: %w", err)
	}
	return info, nil
}

// PodTargets builds a DiscoveryConfig.Targets function for the StatefulSet
// layout of the Kubernetes deployment: match "match-007" is served by pair 7,
// whose pods are "<set>-7" for each set, reachable through a headless service
// at <set>-7.<service>.<namespace>.svc.<clusterDomain>:<port> (the same name
// the failure detector announces).
//
// A match id that does not look like <matchPrefix><number> has no targets.
func PodTargets(matchPrefix string, sets []string, service, namespace, clusterDomain string, port int) func(string) []string {
	return func(matchID string) []string {
		if !strings.HasPrefix(matchID, matchPrefix) {
			return nil
		}
		digits := matchID[len(matchPrefix):]
		ord, err := strconv.ParseUint(digits, 10, 32)
		if err != nil || digits == "" {
			return nil
		}
		out := make([]string, 0, len(sets))
		for _, set := range sets {
			out = append(out, fmt.Sprintf("%s-%d.%s.%s.svc.%s:%d", set, ord, service, namespace, clusterDomain, port))
		}
		return out
	}
}

// StaticTargets returns the same candidates for every match. It is for local
// runs with a handful of pods.
func StaticTargets(addrs ...string) func(string) []string {
	return func(string) []string { return addrs }
}
