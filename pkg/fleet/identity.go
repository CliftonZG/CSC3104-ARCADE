// Package fleet maps a gameserver process onto its place in the Kubernetes
// fleet: which match it serves, which pod is its Primary/Standby partner,
// what role it starts in, and how other components reach it.
//
//
// Topology (go to k8s/README.md):
//
//	StatefulSet arcade-gs-a  (ROLE=primary)   arcade-gs-a-0  arcade-gs-a-1 ... arcade-gs-a-(N-1)
//	StatefulSet arcade-gs-b  (ROLE=backup)    arcade-gs-b-0  arcade-gs-b-1 ... arcade-gs-b-(N-1)
//	                                               |              |
//	                                           match-000      match-001   ...
//
// Pod a-i and pod b-i form pair i and serve match-<i>. Both StatefulSets are
// governed by one headless Service, so every pod has a stable DNS name
// (<pod>.<service>.<namespace>.svc.cluster.local). That gives each pod a
// fixed, known address for its partner, which the UDP heartbeat needs.
package fleet

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Role is the replication role of a gameserver pod.
type Role string

const (
	// RolePrimary serves the match and streams snapshots to Redis.
	RolePrimary Role = "primary"
	// RoleBackup is the hot standby that takes over when the primary fails.
	RoleBackup Role = "backup"
)

// ParseRole accepts "primary", "backup" or "standby" (an alias for backup).
func ParseRole(s string) (Role, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "primary":
		return RolePrimary, nil
	case "backup", "standby":
		return RoleBackup, nil
	default:
		return "", fmt.Errorf("fleet: unknown role %q (want primary or backup)", s)
	}
}

// Default ports shared by every pod in the fleet.
const (
	DefaultHTTPPort      = 8081 // metrics, health, pprof, /role
	DefaultHeartbeatPort = 9000 // UDP heartbeat between the pair (pkg/detector)
	DefaultMatchPrefix   = "match"
	DefaultClusterDomain = "cluster.local"
)

// ErrNoPeer is returned by peer helpers when the pod runs standalone.
var ErrNoPeer = errors.New("fleet: no peer configured (standalone mode)")

// Identity is everything a gameserver needs to know about its own place in
// the fleet. It is resolved once at startup and then read-only.
type Identity struct {
	PodName   string // e.g. arcade-gs-a-3; empty when running outside Kubernetes
	Namespace string
	Service   string // governing headless Service, e.g. arcade-gs

	StatefulSet string // e.g. arcade-gs-a
	Ordinal     int    // pair index; -1 when standalone

	MatchID     string
	InitialRole Role // the role the manifest asked for (ROLE env)

	PeerName string // e.g. arcade-gs-b-3
	PeerHost string // DNS name or IP used to reach the peer

	HTTPPort          int
	HeartbeatPort     int
	PeerHTTPPort      int
	PeerHeartbeatPort int
}

// Standalone reports whether the pod has no partner (local development).
func (id Identity) Standalone() bool { return id.PeerHost == "" }

// PeerHeartbeatAddr is the UDP address of the partner's heartbeat listener.
// Marcus's detector dials this.
func (id Identity) PeerHeartbeatAddr() (string, error) {
	if id.Standalone() {
		return "", ErrNoPeer
	}
	return net.JoinHostPort(id.PeerHost, strconv.Itoa(id.PeerHeartbeatPort)), nil
}

// PeerHTTPAddr is the host:port of the partner's HTTP server (/role, /healthz).
func (id Identity) PeerHTTPAddr() (string, error) {
	if id.Standalone() {
		return "", ErrNoPeer
	}
	return net.JoinHostPort(id.PeerHost, strconv.Itoa(id.PeerHTTPPort)), nil
}

// HeartbeatListenAddr is the local UDP address the heartbeat receiver binds.
func (id Identity) HeartbeatListenAddr() string {
	return net.JoinHostPort("", strconv.Itoa(id.HeartbeatPort))
}

// String gives a one-line summary for startup logs.
func (id Identity) String() string {
	if id.Standalone() {
		return fmt.Sprintf("pod=%q match=%s role=%s (standalone)", id.PodName, id.MatchID, id.InitialRole)
	}
	return fmt.Sprintf("pod=%s pair=%d match=%s role=%s peer=%s", id.PodName, id.Ordinal, id.MatchID, id.InitialRole, id.PeerHost)
}

// Defaults are the values used when the environment does not override them,
// normally taken from the gameserver's command-line flags.
type Defaults struct {
	Role     string
	MatchID  string
	HTTPPort int
}

// Resolve builds the pod's Identity from its environment. getenv is normally
// os.Getenv; tests pass a map lookup.
//
// Environment (all set by k8s/base/gameserver-*.yaml):
//
//	POD_NAME            pod name from the Downward API (arcade-gs-a-3)
//	POD_NAMESPACE       namespace from the Downward API
//	ROLE                initial role: primary | backup
//	PEER_STATEFULSET    the partner StatefulSet (arcade-gs-b for arcade-gs-a)
//	GOVERNING_SERVICE   the headless Service name (arcade-gs)
//	MATCH_PREFIX        match ids are <prefix>-<ordinal, 3 digits> (default "match")
//	MATCH_ID            explicit match id, overrides the ordinal mapping
//	PEER_HOST           explicit peer host/IP, overrides DNS (local testing)
//	HTTP_PORT, HEARTBEAT_PORT, PEER_HTTP_PORT, PEER_HEARTBEAT_PORT
//	CLUSTER_DOMAIN      default cluster.local
func Resolve(getenv func(string) string, def Defaults) (Identity, error) {
	env := func(k, fallback string) string {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return v
		}
		return fallback
	}
	intEnv := func(k string, fallback int) (int, error) {
		v := strings.TrimSpace(getenv(k))
		if v == "" {
			return fallback, nil
		}
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 65535 {
			return 0, fmt.Errorf("fleet: %s=%q is not a valid port", k, v)
		}
		return n, nil
	}

	if def.HTTPPort == 0 {
		def.HTTPPort = DefaultHTTPPort
	}

	id := Identity{
		PodName:   env("POD_NAME", ""),
		Namespace: env("POD_NAMESPACE", "default"),
		Service:   env("GOVERNING_SERVICE", ""),
		Ordinal:   -1,
	}

	role, err := ParseRole(env("ROLE", firstNonEmpty(def.Role, string(RolePrimary))))
	if err != nil {
		return Identity{}, err
	}
	id.InitialRole = role

	if id.HTTPPort, err = intEnv("HTTP_PORT", def.HTTPPort); err != nil {
		return Identity{}, err
	}
	if id.HeartbeatPort, err = intEnv("HEARTBEAT_PORT", DefaultHeartbeatPort); err != nil {
		return Identity{}, err
	}
	if id.PeerHTTPPort, err = intEnv("PEER_HTTP_PORT", id.HTTPPort); err != nil {
		return Identity{}, err
	}
	if id.PeerHeartbeatPort, err = intEnv("PEER_HEARTBEAT_PORT", id.HeartbeatPort); err != nil {
		return Identity{}, err
	}

	// StatefulSet pods are named <statefulset>-<ordinal>.
	if id.PodName != "" {
		if set, ord, ok := splitOrdinal(id.PodName); ok {
			id.StatefulSet, id.Ordinal = set, ord
		}
	}

	switch {
	case getenv("MATCH_ID") != "":
		id.MatchID = strings.TrimSpace(getenv("MATCH_ID"))
	case id.Ordinal >= 0:
		id.MatchID = MatchIDFor(env("MATCH_PREFIX", DefaultMatchPrefix), id.Ordinal)
	default:
		id.MatchID = firstNonEmpty(def.MatchID, "match-default")
	}

	// Peer: an explicit PEER_HOST wins (docker-compose / local runs); otherwise
	// derive the partner's stable DNS name from the StatefulSet layout.
	peerSet := env("PEER_STATEFULSET", "")
	if peerSet != "" && id.Ordinal >= 0 {
		id.PeerName = fmt.Sprintf("%s-%d", peerSet, id.Ordinal)
	}
	switch {
	case getenv("PEER_HOST") != "":
		id.PeerHost = strings.TrimSpace(getenv("PEER_HOST"))
	case id.PeerName != "" && id.Service != "":
		id.PeerHost = PodFQDN(id.PeerName, id.Service, id.Namespace, env("CLUSTER_DOMAIN", DefaultClusterDomain))
	}

	return id, nil
}

// MatchIDFor returns the match id served by pair ordinal, e.g. match-007.
func MatchIDFor(prefix string, ordinal int) string {
	return fmt.Sprintf("%s-%03d", prefix, ordinal)
}

// PodFQDN is the stable DNS name a headless Service gives a StatefulSet pod.
func PodFQDN(pod, service, namespace, clusterDomain string) string {
	return fmt.Sprintf("%s.%s.%s.svc.%s", pod, service, namespace, clusterDomain)
}

// splitOrdinal splits "arcade-gs-a-12" into ("arcade-gs-a", 12).
func splitOrdinal(pod string) (string, int, bool) {
	i := strings.LastIndexByte(pod, '-')
	if i <= 0 || i == len(pod)-1 {
		return "", 0, false
	}
	n, err := strconv.Atoi(pod[i+1:])
	if err != nil || n < 0 {
		return "", 0, false
	}
	return pod[:i], n, true
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}
