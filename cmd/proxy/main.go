// Command proxy is the ARCADE ingress: it keeps players' WebSockets open and
// follows the control plane's primary-change notifications.
//
//	proxy -listen :8080 -admin 127.0.0.1:9090 -default-upstream game-0.game:8081
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/CSC3104-ARCADE/pkg/proxy"
	"github.com/CSC3104-ARCADE/pkg/webui"
)

// listFlag is a flag that can be given several times.
type listFlag []string

func (l *listFlag) String() string     { return strings.Join(*l, ",") }
func (l *listFlag) Set(v string) error { *l = append(*l, v); return nil }

// options is everything that can be configured from the command line.
type options struct {
	listen, admin, defaultUpstream, upstreamPath string
	port                                         int
	adminToken                                   string
	origins                                      listFlag
	insecureOrigin                               bool
	routes                                       listFlag
	logLevel                                     string
	logJSON                                      bool
	cfg                                          proxy.Config
	shutdownTimeout                              time.Duration

	discover      bool
	discoverPods  listFlag
	discoverSets  string
	discoverSvc   string
	discoverNS    string
	discoverPort  int
	discoverMatch string
	discoverDom   string
	discoverEvery time.Duration
	discoverTO    time.Duration
}

func parseFlags(args []string, stderr io.Writer) (*options, error) {
	var o options
	fs := flag.NewFlagSet("proxy", flag.ContinueOnError)
	fs.SetOutput(stderr)

	fs.StringVar(&o.listen, "listen", ":8080", "address for client WebSocket connections (path /ws?match=<id>)")
	fs.IntVar(&o.port, "port", 0, "shorthand for -listen :PORT (kept for the Makefile's run-proxy target)")
	fs.StringVar(&o.admin, "admin", "127.0.0.1:9090", "address for the admin API; keep it internal. Empty disables it")
	fs.StringVar(&o.defaultUpstream, "default-upstream", "", "host:port used for matches that have no route yet (single-pair setups, local dev)")
	fs.StringVar(&o.defaultUpstream, "primary", "", "alias of -default-upstream")
	fs.StringVar(&o.upstreamPath, "upstream-path", "/ws", "WebSocket path on the game pods")
	fs.StringVar(&o.adminToken, "admin-token", os.Getenv("ARCADE_ADMIN_TOKEN"), "bearer token required on /admin/ endpoints (default: $ARCADE_ADMIN_TOKEN)")
	fs.Var(&o.origins, "origin", "allowed browser Origin host pattern, e.g. game.example.com or *.example.com (repeatable)")
	fs.BoolVar(&o.insecureOrigin, "insecure-skip-origin-check", false, "accept WebSockets from any Origin (development only)")
	fs.Var(&o.routes, "route", "initial route as match=epoch@host:port (repeatable)")
	fs.StringVar(&o.logLevel, "log-level", "info", "debug, info, warn or error")
	fs.BoolVar(&o.logJSON, "log-json", false, "log as JSON")
	fs.DurationVar(&o.shutdownTimeout, "shutdown-timeout", 10*time.Second, "how long to wait for clients to close on SIGTERM")

	fs.BoolVar(&o.discover, "discover", false, "find each match's primary by asking its pods for GET /role (Kubernetes StatefulSet layout; see -discover-* flags)")
	fs.Var(&o.discoverPods, "discover-pods", "candidate pods host:port for every match, instead of the Kubernetes layout (repeatable; local runs)")
	fs.StringVar(&o.discoverSets, "discover-sets", os.Getenv("GAMESERVER_SETS"), "comma-separated StatefulSet names of the pair, e.g. arcade-gs-a,arcade-gs-b (default $GAMESERVER_SETS)")
	fs.StringVar(&o.discoverSvc, "discover-service", envOr("GAMESERVER_SERVICE", "arcade-gs"), "headless service of the game pods (default $GAMESERVER_SERVICE or arcade-gs)")
	fs.StringVar(&o.discoverNS, "discover-namespace", envOr("POD_NAMESPACE", "arcade"), "namespace of the game pods (default $POD_NAMESPACE or arcade)")
	fs.IntVar(&o.discoverPort, "discover-port", envInt("GAMESERVER_HTTP_PORT", 8081), "game pod HTTP port (default $GAMESERVER_HTTP_PORT or 8081)")
	fs.StringVar(&o.discoverMatch, "discover-match-prefix", "match-", "match ids are <prefix><pair number>, e.g. match-007 is pair 7")
	fs.StringVar(&o.discoverDom, "discover-cluster-domain", "cluster.local", "cluster DNS domain")
	fs.DurationVar(&o.discoverEvery, "discover-interval", time.Second, "how often matches with clients are polled")
	fs.DurationVar(&o.discoverTO, "discover-timeout", 800*time.Millisecond, "timeout of one GET /role")

	c := &o.cfg
	fs.DurationVar(&c.DialTimeout, "dial-timeout", 0, "timeout for one attempt to reach a game pod (default 3s)")
	fs.DurationVar(&c.WriteTimeout, "write-timeout", 0, "timeout for one write to a client or game pod (default 10s)")
	fs.DurationVar(&c.MaxOutage, "max-outage", 0, "close a client with 1013 after this long without a primary (default 15s)")
	fs.IntVar(&c.PendingMaxMessages, "pending-messages", 0, "messages buffered per client while there is no primary (default 256)")
	fs.IntVar(&c.PendingMaxBytes, "pending-bytes", 0, "bytes buffered per client while there is no primary (default 1048576)")
	fs.DurationVar(&c.BackoffMin, "backoff-min", 0, "first retry delay when a primary is unreachable (default 50ms)")
	fs.DurationVar(&c.BackoffMax, "backoff-max", 0, "longest retry delay (default 1s)")
	fs.DurationVar(&c.ClientPingInterval, "client-ping", 0, "ping clients this often; negative disables (default 25s)")
	fs.DurationVar(&c.UpstreamPingInterval, "upstream-ping", 0, "ping game pods this often; negative disables (default 5s)")
	fs.DurationVar(&c.PingTimeout, "ping-timeout", 0, "drop a peer that does not answer a ping in this time (default 5s)")
	fs.IntVar(&c.MaxSessions, "max-sessions", 0, "maximum concurrent clients; 0 = unlimited")
	fs.Int64Var(&c.ClientReadLimit, "client-read-limit", 0, "largest message accepted from a client, bytes (default 65536)")
	fs.Int64Var(&c.UpstreamReadLimit, "upstream-read-limit", 0, "largest message accepted from a game pod, bytes (default 1048576)")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}
	if fs.NArg() != 0 {
		return nil, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	}
	if o.port != 0 {
		o.listen = fmt.Sprintf(":%d", o.port)
	}
	return &o, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

// discoveryConfig returns the discovery settings, or nil if discovery is off.
func (o *options) discoveryConfig() (*proxy.DiscoveryConfig, error) {
	if !o.discover && len(o.discoverPods) == 0 {
		return nil, nil
	}
	cfg := &proxy.DiscoveryConfig{Interval: o.discoverEvery, Timeout: o.discoverTO}
	if len(o.discoverPods) > 0 {
		for _, a := range o.discoverPods {
			if err := proxy.ValidAddr(a); err != nil {
				return nil, fmt.Errorf("-discover-pods: %w", err)
			}
		}
		cfg.Targets = proxy.StaticTargets(o.discoverPods...)
		return cfg, nil
	}
	var sets []string
	for _, s := range strings.Split(o.discoverSets, ",") {
		if s = strings.TrimSpace(s); s != "" {
			sets = append(sets, s)
		}
	}
	if len(sets) == 0 {
		return nil, errors.New("-discover needs -discover-sets (or $GAMESERVER_SETS), e.g. arcade-gs-a,arcade-gs-b")
	}
	cfg.Targets = proxy.PodTargets(o.discoverMatch, sets, o.discoverSvc, o.discoverNS, o.discoverDom, o.discoverPort)
	return cfg, nil
}

// parseRoute parses "match=epoch@host:port".
func parseRoute(s string) (proxy.Update, error) {
	match, rest, ok := strings.Cut(s, "=")
	if !ok {
		return proxy.Update{}, fmt.Errorf("route %q: want match=epoch@host:port", s)
	}
	epochStr, addr, ok := strings.Cut(rest, "@")
	if !ok {
		return proxy.Update{}, fmt.Errorf("route %q: want match=epoch@host:port", s)
	}
	epoch, err := strconv.ParseUint(epochStr, 10, 64)
	if err != nil {
		return proxy.Update{}, fmt.Errorf("route %q: bad epoch: %w", s, err)
	}
	return proxy.Update{MatchID: match, Epoch: epoch, PrimaryAddr: addr}, nil
}

func newLogger(o *options, w io.Writer) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(o.logLevel)); err != nil {
		return nil, fmt.Errorf("bad -log-level %q", o.logLevel)
	}
	hopts := &slog.HandlerOptions{Level: lvl}
	if o.logJSON {
		return slog.New(slog.NewJSONHandler(w, hopts)), nil
	}
	return slog.New(slog.NewTextHandler(w, hopts)), nil
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, "proxy:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stderr io.Writer) error {
	o, err := parseFlags(args, stderr)
	if err != nil {
		return err
	}
	log, err := newLogger(o, stderr)
	if err != nil {
		return err
	}

	o.cfg.UpstreamPath = o.upstreamPath
	o.cfg.OriginPatterns = o.origins
	o.cfg.InsecureSkipOriginCheck = o.insecureOrigin
	o.cfg.AdminToken = o.adminToken
	o.cfg.Logger = log
	p := proxy.New(o.cfg)

	if o.defaultUpstream != "" {
		if err := p.Table().SetDefault(o.defaultUpstream); err != nil {
			return fmt.Errorf("-default-upstream: %w", err)
		}
	}
	for _, r := range o.routes {
		u, err := parseRoute(r)
		if err != nil {
			return err
		}
		if _, err := p.ApplyUpdate(u); err != nil {
			return fmt.Errorf("-route %q: %w", r, err)
		}
	}
	dcfg, err := o.discoveryConfig()
	if err != nil {
		return err
	}
	if o.insecureOrigin {
		log.Warn("origin checking is disabled")
	}
	if o.admin != "" && o.adminToken == "" {
		log.Warn("admin API has no token; make sure it is only reachable from the control plane", "admin", o.admin)
	}

	mux := http.NewServeMux()
	mux.Handle("/", webui.Handler())
	mux.Handle("/ws", p)
	mux.HandleFunc("/api/rooms", p.HandleRooms)
	mux.HandleFunc("/api/matchmake", p.HandleMatchmake)
	mux.HandleFunc("/api/cluster", p.HandleCluster)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	clientSrv := &http.Server{Addr: o.listen, Handler: mux, ReadHeaderTimeout: 10 * time.Second}

	var adminSrv *http.Server
	if o.admin != "" {
		adminSrv = &http.Server{Addr: o.admin, Handler: p.AdminHandler(), ReadHeaderTimeout: 10 * time.Second}
	}

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	errc := make(chan error, 2)
	go func() { errc <- fmt.Errorf("client listener: %w", clientSrv.ListenAndServe()) }()
	if adminSrv != nil {
		go func() { errc <- fmt.Errorf("admin listener: %w", adminSrv.ListenAndServe()) }()
	}
	if dcfg != nil {
		d := proxy.NewDiscoverer(p, *dcfg)
		go d.Run(ctx)
		log.Info("discovering primaries via GET /role", "interval", dcfg.Interval)
	}
	log.Info("proxy started", "listen", o.listen, "admin", o.admin, "default_upstream", o.defaultUpstream)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}

	log.Info("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), o.shutdownTimeout)
	defer cancel()
	if err := p.Shutdown(sctx); err != nil {
		log.Warn("some clients did not close in time", "err", err)
	}
	_ = clientSrv.Shutdown(sctx)
	if adminSrv != nil {
		_ = adminSrv.Shutdown(sctx)
	}
	return nil
}
