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
	if o.insecureOrigin {
		log.Warn("origin checking is disabled")
	}
	if o.admin != "" && o.adminToken == "" {
		log.Warn("admin API has no token; make sure it is only reachable from the control plane", "admin", o.admin)
	}

	mux := http.NewServeMux()
	mux.Handle("/ws", p)
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
