package fleet

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	isPrimaryGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "arcade",
		Subsystem: "fleet",
		Name:      "is_primary",
		Help:      "1 if this pod currently holds the primary role for its match, 0 if it is the standby.",
	})
	roleTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "arcade",
		Subsystem: "fleet",
		Name:      "role_transitions_total",
		Help:      "Role changes on this pod (promotion to primary, demotion to backup).",
	}, []string{"to"})
)

// RoleInfo is the JSON document served at GET /role. The proxy and the
// partner pod use it to find out who currently owns a match.
type RoleInfo struct {
	Pod       string    `json:"pod"`
	MatchID   string    `json:"match_id"`
	Role      Role      `json:"role"`
	Epoch     int64     `json:"epoch"`
	Ready     bool      `json:"ready"`
	Peer      string    `json:"peer,omitempty"`
	Since     time.Time `json:"since"`
	StartedAt time.Time `json:"started_at"`
}

// Status is the live, thread-safe role of this pod. Startup sets the initial
// role; the failure detector calls Promote when the standby takes over; the
// gameserver calls Demote when its epoch is fenced.
type Status struct {
	mu        sync.RWMutex
	id        Identity
	role      Role
	epoch     int64
	ready     bool
	since     time.Time
	startedAt time.Time
	listeners []func(RoleInfo)
}

// NewStatus creates the status for id, starting in role.
func NewStatus(id Identity, role Role) *Status {
	now := time.Now()
	s := &Status{id: id, role: role, since: now, startedAt: now}
	s.updateGauge(role)
	return s
}

// Identity returns the identity the status was created with.
func (s *Status) Identity() Identity { return s.id }

// Role returns the current role.
func (s *Status) Role() Role {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.role
}

// IsPrimary is shorthand for Role() == RolePrimary.
func (s *Status) IsPrimary() bool { return s.Role() == RolePrimary }

// Info returns a snapshot of the status.
func (s *Status) Info() RoleInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.infoLocked()
}

func (s *Status) infoLocked() RoleInfo {
	return RoleInfo{
		Pod:       s.id.PodName,
		MatchID:   s.id.MatchID,
		Role:      s.role,
		Epoch:     s.epoch,
		Ready:     s.ready,
		Peer:      s.id.PeerHost,
		Since:     s.since,
		StartedAt: s.startedAt,
	}
}

// OnChange registers fn to run (in its own goroutine) after every change of
// role, epoch or readiness. It is called once immediately with the current
// state so late subscribers (e.g. the pod labeler) start in sync.
func (s *Status) OnChange(fn func(RoleInfo)) {
	s.mu.Lock()
	s.listeners = append(s.listeners, fn)
	info := s.infoLocked()
	s.mu.Unlock()
	go fn(info)
}

// SetEpoch records the fencing epoch this pod holds (primary only).
func (s *Status) SetEpoch(epoch int64) {
	s.update(func() bool {
		if s.epoch == epoch {
			return false
		}
		s.epoch = epoch
		return true
	})
}

// MarkReady flags that the engine is initialised and the pod can take traffic.
func (s *Status) MarkReady() {
	s.update(func() bool {
		if s.ready {
			return false
		}
		s.ready = true
		return true
	})
}

// Ready reports whether MarkReady has been called.
func (s *Status) Ready() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}

// Promote switches the pod to primary holding epoch. The failure detector
// calls this after replication.Store.TakeOver succeeds.
func (s *Status) Promote(epoch int64) { s.setRole(RolePrimary, epoch) }

// Demote switches the pod to backup, e.g. after its epoch was fenced.
func (s *Status) Demote() { s.setRole(RoleBackup, 0) }

func (s *Status) setRole(r Role, epoch int64) {
	s.update(func() bool {
		if s.role == r && s.epoch == epoch {
			return false
		}
		if s.role != r {
			s.since = time.Now()
			roleTransitions.WithLabelValues(string(r)).Inc()
		}
		s.role = r
		s.epoch = epoch
		s.updateGauge(r)
		return true
	})
}

func (s *Status) update(mutate func() bool) {
	s.mu.Lock()
	if !mutate() {
		s.mu.Unlock()
		return
	}
	info := s.infoLocked()
	listeners := append([]func(RoleInfo){}, s.listeners...)
	s.mu.Unlock()
	for _, fn := range listeners {
		go fn(info)
	}
}

func (s *Status) updateGauge(r Role) {
	if r == RolePrimary {
		isPrimaryGauge.Set(1)
	} else {
		isPrimaryGauge.Set(0)
	}
}

// RoleHandler serves GET /role as JSON.
func (s *Status) RoleHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(s.Info())
	})
}

// ReadyHandler serves GET /readyz: 200 once the engine is up, 503 before.
// Kubernetes uses it as the readiness probe. Both roles report ready: the
// standby must be Ready too, or a rolling update would never progress.
func (s *Status) ReadyHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !s.Ready() {
			http.Error(w, "engine starting", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("OK"))
	})
}

// Register mounts /role and /readyz on mux.
func (s *Status) Register(mux *http.ServeMux) {
	mux.Handle("/role", s.RoleHandler())
	mux.Handle("/readyz", s.ReadyHandler())
}
