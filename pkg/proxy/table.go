package proxy

import (
	"fmt"
	"net"
	"strconv"
	"sync"
)

// Route says which game pod is the primary for a match, and under which
// fencing epoch it was promoted.
type Route struct {
	Epoch uint64 `json:"epoch"`
	Addr  string `json:"primary_addr"` // host:port of the primary game pod
}

// Update is a takeover notification from the control plane. This is the wire
// format of the interface between the leader-election service and the proxy
// (admin API body, or Redis pub/sub message).
type Update struct {
	MatchID     string `json:"match_id"`
	Epoch       uint64 `json:"epoch"`
	PrimaryAddr string `json:"primary_addr"`
}

// ApplyResult says what Table.Apply did with an Update.
type ApplyResult string

const (
	// Applied: the update was newer and replaced the route.
	Applied ApplyResult = "applied"
	// Stale: the update's epoch is older than the current one; ignored.
	Stale ApplyResult = "stale"
	// Duplicate: same epoch and same address as the current route; no-op.
	Duplicate ApplyResult = "duplicate"
	// Conflict: same epoch but a different address. Two primaries for one
	// epoch means the control plane has a bug; the update is ignored.
	Conflict ApplyResult = "conflict"
)

// MaxMatchIDLen is the longest accepted match id.
const MaxMatchIDLen = 64

// ValidMatchID reports whether id is 1..MaxMatchIDLen characters from
// [A-Za-z0-9_-]. Keeping ids to this alphabet means they are safe in logs,
// URLs and Redis keys.
func ValidMatchID(id string) bool {
	if len(id) == 0 || len(id) > MaxMatchIDLen {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_', c == '-':
		default:
			return false
		}
	}
	return true
}

// ValidAddr reports whether addr is a plain host:port (no scheme, no path).
func ValidAddr(addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("address %q is not host:port: %w", addr, err)
	}
	if host == "" {
		return fmt.Errorf("address %q has no host", addr)
	}
	for i := 0; i < len(host); i++ {
		c := host[i]
		if c <= ' ' || c == '/' || c == '@' || c == '?' || c == '#' || c == 0x7f {
			return fmt.Errorf("address %q has an invalid character in the host", addr)
		}
	}
	if p, err := strconv.ParseUint(port, 10, 16); err != nil || p == 0 {
		return fmt.Errorf("address %q has an invalid port", addr)
	}
	return nil
}

// Table maps match ids to their current primary. It is safe for concurrent
// use.
//
// The rule that makes takeover safe under message reordering and duplicate
// delivery: a route is only ever replaced by one with a strictly higher
// epoch.
type Table struct {
	mu       sync.RWMutex
	routes   map[string]Route
	watchers map[string]map[*Watcher]struct{}
	def      string
}

// NewTable returns an empty table.
func NewTable() *Table {
	return &Table{
		routes:   make(map[string]Route),
		watchers: make(map[string]map[*Watcher]struct{}),
	}
}

// SetDefault sets a fallback primary address used for matches that have no
// route of their own (epoch 0). An empty string disables the fallback. It is
// meant for single-pair setups and local development.
func (t *Table) SetDefault(addr string) error {
	if addr != "" {
		if err := ValidAddr(addr); err != nil {
			return err
		}
	}
	t.mu.Lock()
	t.def = addr
	t.mu.Unlock()
	return nil
}

// Apply installs the update if it is newer than the current route and wakes
// every session watching that match.
func (t *Table) Apply(u Update) (ApplyResult, error) {
	if !ValidMatchID(u.MatchID) {
		return "", fmt.Errorf("invalid match_id %q", u.MatchID)
	}
	if err := ValidAddr(u.PrimaryAddr); err != nil {
		return "", err
	}

	t.mu.Lock()
	cur, exists := t.routes[u.MatchID]
	if exists {
		switch {
		case u.Epoch < cur.Epoch:
			t.mu.Unlock()
			return Stale, nil
		case u.Epoch == cur.Epoch && u.PrimaryAddr == cur.Addr:
			t.mu.Unlock()
			return Duplicate, nil
		case u.Epoch == cur.Epoch:
			t.mu.Unlock()
			return Conflict, nil
		}
	}
	t.routes[u.MatchID] = Route{Epoch: u.Epoch, Addr: u.PrimaryAddr}
	for w := range t.watchers[u.MatchID] {
		w.notify()
	}
	t.mu.Unlock()
	return Applied, nil
}

// Lookup returns the current route for a match. If the match has no route and
// a default is set, the default is returned with epoch 0.
func (t *Table) Lookup(matchID string) (Route, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if r, ok := t.routes[matchID]; ok {
		return r, true
	}
	if t.def != "" {
		return Route{Addr: t.def}, true
	}
	return Route{}, false
}

// Snapshot returns a copy of all routes.
func (t *Table) Snapshot() map[string]Route {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]Route, len(t.routes))
	for k, v := range t.routes {
		out[k] = v
	}
	return out
}

// Default returns the fallback primary address, or "" if none is set.
func (t *Table) Default() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.def
}

// Watcher is notified when the route for one match changes.
type Watcher struct {
	t     *Table
	match string
	ch    chan struct{}
}

// Watch registers a watcher for matchID. The watcher must be stopped.
//
// Register the watcher before the first Lookup: a change that lands between
// the two is then still signalled, so no update can be missed.
func (t *Table) Watch(matchID string) *Watcher {
	w := &Watcher{t: t, match: matchID, ch: make(chan struct{}, 1)}
	t.mu.Lock()
	set := t.watchers[matchID]
	if set == nil {
		set = make(map[*Watcher]struct{})
		t.watchers[matchID] = set
	}
	set[w] = struct{}{}
	t.mu.Unlock()
	return w
}

// C receives a value whenever the route may have changed. Signals coalesce:
// several updates in a row can produce a single wake-up, so after receiving,
// always Lookup the current value instead of counting signals.
func (w *Watcher) C() <-chan struct{} { return w.ch }

// Stop unregisters the watcher.
func (w *Watcher) Stop() {
	w.t.mu.Lock()
	if set := w.t.watchers[w.match]; set != nil {
		delete(set, w)
		if len(set) == 0 {
			delete(w.t.watchers, w.match)
		}
	}
	w.t.mu.Unlock()
}

func (w *Watcher) notify() {
	select {
	case w.ch <- struct{}{}:
	default: // a wake-up is already pending
	}
}
