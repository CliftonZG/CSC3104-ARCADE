package proxy

import (
	"io"
	"log/slog"
	"sync/atomic"
	"time"
)

// Config tunes the proxy. The zero value is usable: every zero field is
// replaced by the default shown next to it.
type Config struct {
	// UpstreamPath is the WebSocket path on the game pods. Default "/ws".
	UpstreamPath string

	// DialTimeout bounds one attempt to connect and handshake with a game
	// pod. Default 3s.
	DialTimeout time.Duration
	// WriteTimeout bounds a single write to a client or a game pod. A peer
	// that does not drain within it is treated as dead. Default 10s.
	WriteTimeout time.Duration

	// MaxOutage is how long a client may be left without a primary before
	// the proxy gives up and closes it with 1013 (try again later). It
	// starts when the match has no usable primary and resets once one is
	// connected. Default 15s.
	MaxOutage time.Duration

	// PendingMaxMessages and PendingMaxBytes bound what is buffered for a
	// client while its match has no primary. Overflow closes the client with
	// 1013. Defaults 256 messages and 1 MiB.
	PendingMaxMessages int
	PendingMaxBytes    int

	// BackoffMin and BackoffMax bound the delay between failed attempts to
	// reach a primary (exponential, jittered). A route change cuts any wait
	// short. Defaults 50ms and 1s.
	BackoffMin time.Duration
	BackoffMax time.Duration

	// ClientReadLimit and UpstreamReadLimit are the largest message accepted
	// from clients and from game pods. Defaults 64 KiB and 1 MiB.
	ClientReadLimit   int64
	UpstreamReadLimit int64

	// ClientPingInterval and UpstreamPingInterval control keep-alive pings,
	// which detect peers that vanished without closing (NAT timeouts,
	// network partitions). A peer that does not answer within PingTimeout is
	// dropped. Defaults 25s, 5s and 5s. A negative interval disables pings.
	ClientPingInterval   time.Duration
	UpstreamPingInterval time.Duration
	PingTimeout          time.Duration

	// MaxSessions caps concurrent client connections; 0 means unlimited.
	MaxSessions int

	// OriginPatterns / InsecureSkipOriginCheck control which browser origins
	// may open a WebSocket (see ws.AcceptOptions).
	OriginPatterns          []string
	InsecureSkipOriginCheck bool

	// AdminToken, if set, is required as "Authorization: Bearer <token>" on
	// the admin API's /admin/ endpoints.
	AdminToken string

	// Logger receives operational logs. Default: discard.
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.UpstreamPath == "" {
		c.UpstreamPath = "/ws"
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 3 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 10 * time.Second
	}
	if c.MaxOutage <= 0 {
		c.MaxOutage = 15 * time.Second
	}
	if c.PendingMaxMessages <= 0 {
		c.PendingMaxMessages = 256
	}
	if c.PendingMaxBytes <= 0 {
		c.PendingMaxBytes = 1 << 20
	}
	if c.BackoffMin <= 0 {
		c.BackoffMin = 50 * time.Millisecond
	}
	if c.BackoffMax <= 0 {
		c.BackoffMax = time.Second
	}
	if c.BackoffMax < c.BackoffMin {
		c.BackoffMax = c.BackoffMin
	}
	if c.ClientReadLimit <= 0 {
		c.ClientReadLimit = 64 << 10
	}
	if c.UpstreamReadLimit <= 0 {
		c.UpstreamReadLimit = 1 << 20
	}
	if c.ClientPingInterval == 0 {
		c.ClientPingInterval = 25 * time.Second
	}
	if c.UpstreamPingInterval == 0 {
		c.UpstreamPingInterval = 5 * time.Second
	}
	if c.PingTimeout <= 0 {
		c.PingTimeout = 5 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
}

// Stats are cumulative counters plus a gauge for live sessions.
type Stats struct {
	ActiveSessions  atomic.Int64 // gauge
	TotalSessions   atomic.Int64
	Retargets       atomic.Int64 // times a session moved to a different primary
	DialFailures    atomic.Int64
	FencedDrops     atomic.Int64 // upstream messages dropped because their primary was revoked
	ClientMessages  atomic.Int64 // client -> upstream
	UpstreamMessage atomic.Int64 // upstream -> client
	BufferedMsgs    atomic.Int64 // client messages that had to wait for a primary
	OutageTimeouts  atomic.Int64 // clients closed after MaxOutage
	BufferOverflows atomic.Int64
	RoutesApplied   atomic.Int64
	RoutesIgnored   atomic.Int64 // stale / duplicate / conflicting updates
}

// StatsSnapshot is the JSON form of Stats.
type StatsSnapshot struct {
	ActiveSessions   int64 `json:"active_sessions"`
	TotalSessions    int64 `json:"total_sessions"`
	Retargets        int64 `json:"retargets"`
	DialFailures     int64 `json:"dial_failures"`
	FencedDrops      int64 `json:"fenced_drops"`
	ClientMessages   int64 `json:"client_messages"`
	UpstreamMessages int64 `json:"upstream_messages"`
	BufferedMessages int64 `json:"buffered_messages"`
	OutageTimeouts   int64 `json:"outage_timeouts"`
	BufferOverflows  int64 `json:"buffer_overflows"`
	RoutesApplied    int64 `json:"routes_applied"`
	RoutesIgnored    int64 `json:"routes_ignored"`
}

// Snapshot returns a consistent-enough copy of the counters.
func (s *Stats) Snapshot() StatsSnapshot {
	return StatsSnapshot{
		ActiveSessions:   s.ActiveSessions.Load(),
		TotalSessions:    s.TotalSessions.Load(),
		Retargets:        s.Retargets.Load(),
		DialFailures:     s.DialFailures.Load(),
		FencedDrops:      s.FencedDrops.Load(),
		ClientMessages:   s.ClientMessages.Load(),
		UpstreamMessages: s.UpstreamMessage.Load(),
		BufferedMessages: s.BufferedMsgs.Load(),
		OutageTimeouts:   s.OutageTimeouts.Load(),
		BufferOverflows:  s.BufferOverflows.Load(),
		RoutesApplied:    s.RoutesApplied.Load(),
		RoutesIgnored:    s.RoutesIgnored.Load(),
	}
}
