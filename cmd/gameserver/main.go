package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof" // Registers /debug/pprof handlers for CPU/heap profiling
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/CSC3104-ARCADE/pkg/detector"
	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/loadgen"
	"github.com/CSC3104-ARCADE/pkg/metrics"
	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	role := flag.String("role", "primary", "pod role: primary or backup")
	matchID := flag.String("match", "match-default", "match identifier")
	seed := flag.Int64("seed", 42, "initial shuffle seed")
	httpPort := flag.Int("http", 8081, "HTTP port for metrics, health, and pprof")
	redisAddr := flag.String("redis", "", "Redis address for snapshot replication (empty disables replication)")
	redisSentinels := flag.String("redis-sentinels", "", "comma-separated Redis Sentinel addresses; overrides -redis with a highly available primary/replica group")
	redisMaster := flag.String("redis-master", "arcade", "Redis Sentinel master name")
	redisAcks := flag.Int("redis-acks", 0, "replicas that must acknowledge every epoch claim and snapshot (Redis WAIT); 0 disables")
	redisAckTimeout := flag.Duration("redis-ack-timeout", 250*time.Millisecond, "how long to wait for -redis-acks replicas")
	redisWriteTimeout := flag.Duration("redis-write-timeout", 0, "how long a move's snapshot commit is retried (0: 3s with Sentinel, else 500ms)")
	snapshotFormat := flag.String("snapshot-format", "json", "replicated snapshot encoding: json or binary")
	autoplay := flag.Duration("autoplay", 0, "if > 0, the primary self-plays one synthetic move per interval (load testing)")
	flag.Parse()

	// Environment variable overrides for Kubernetes orchestration
	if envRole := os.Getenv("ROLE"); envRole != "" {
		*role = envRole
	}
	if envMatch := os.Getenv("MATCH_ID"); envMatch != "" {
		*matchID = envMatch
	}
	if envRedis := os.Getenv("REDIS_ADDR"); envRedis != "" {
		*redisAddr = envRedis
	}
	if env := os.Getenv("REDIS_SENTINELS"); env != "" {
		*redisSentinels = env
	}
	if env := os.Getenv("REDIS_MASTER_NAME"); env != "" {
		*redisMaster = env
	}
	if env := os.Getenv("REDIS_MIN_REPLICA_ACKS"); env != "" {
		n, perr := strconv.Atoi(env)
		if perr != nil {
			log.Fatalf("[GAMESERVER] invalid REDIS_MIN_REPLICA_ACKS %q: %v", env, perr)
		}
		*redisAcks = n
	}
	redisCfg := redisConfig{Addr: *redisAddr, Sentinels: splitAddrs(*redisSentinels), MasterName: *redisMaster,
		MinAcks: *redisAcks, AckTimeout: *redisAckTimeout, WriteTimeout: *redisWriteTimeout}
	if envAutoplay := os.Getenv("AUTOPLAY_INTERVAL"); envAutoplay != "" {
		d, perr := time.ParseDuration(envAutoplay)
		if perr != nil {
			log.Fatalf("[GAMESERVER] invalid AUTOPLAY_INTERVAL %q: %v", envAutoplay, perr)
		}
		*autoplay = d
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Fleet identity: which match this pod serves, who its partner is, and
	// which role it should really start in (see pkg/fleet).
	identity, err := fleet.Resolve(os.Getenv, fleet.Defaults{Role: *role, MatchID: *matchID, HTTPPort: *httpPort})
	if err != nil {
		log.Fatalf("[GAMESERVER] Invalid fleet configuration: %v", err)
	}
	decision := fleet.Arbitrate(ctx, identity, fleet.ArbitrateConfig{Logf: log.Printf})
	log.Printf("[FLEET] %s -> starting as %s: %s", identity, decision.Role, decision.Reason)
	*role, *matchID, *httpPort = string(decision.Role), identity.MatchID, identity.HTTPPort
	status := fleet.NewStatus(identity, decision.Role)

	log.Printf("[GAMESERVER] Booting pod role=%s match=%s seed=%d http=:%d", *role, *matchID, *seed, *httpPort)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	newGame := func() (*uno.Game, error) {
		return uno.NewGameEngine(*matchID, []string{"player1", "player2", "player3", "player4"}, *seed)
	}

	var engine *uno.Game
	var store *replication.Store

	// Connect to Redis for both Primary and Standby pods if Redis is configured
	if redisCfg.enabled() {
		store, err = connectStoreWithRetry(ctx, redisCfg, 60*time.Second)
		if err != nil {
			log.Fatalf("[GAMESERVER] Failed to connect to Redis: %v", err)
		}
	} else {
		log.Printf("[GAMESERVER] Replication disabled (no -redis / REDIS_ADDR)")
	}

	switch {
	case store == nil:
		engine, err = newGame()
	case *role == "primary":
		engine, err = startPrimary(ctx, store, *matchID, replication.Format(*snapshotFormat), newGame,
			func(fe *replication.FencedError) {
				// A newer primary owns the match; this pod's state is no longer authoritative.
				log.Printf("[REPLICATION] %v. Stepping down.", fe)
				status.Demote()
				select {
				case sigCh <- syscall.SIGTERM:
				default:
				}
			})
		if err == nil {
			if epoch, eerr := store.CurrentEpoch(ctx, *matchID); eerr == nil {
				status.SetEpoch(epoch)
			}
		}
	default:
		// Standby: promotion (store.TakeOver) is driven by the failure detector.
		log.Printf("[GAMESERVER] Standby for match=%s; replication source redis=%s", *matchID, redisCfg)
		engine, err = newGame()
	}
	if err != nil {
		log.Fatalf("[GAMESERVER] Failed to initialize engine: %v", err)
	}

	metrics.ActiveMatches.Inc()
	defer metrics.ActiveMatches.Dec()

	chk, _ := engine.Checksum()
	log.Printf("[GAMESERVER] State engine online. Initial sequence=%d SHA256=%s", engine.Sequence(), chk)

	// Encapsulate engine in a thread-safe, decoupled GameSession
	session := NewGameSession(engine)

	// Register persistent move hook for Prometheus metrics instrumentation
	session.RegisterHook(func(prev, next *uno.State, summary uno.ActionSummary) error {
		start := time.Now()
		defer func() {
			elapsedUs := float64(time.Since(start).Microseconds())
			metrics.TurnExecutionLatency.WithLabelValues(string(summary.Action.Type)).Observe(elapsedUs)
			metrics.TotalTurnsCommitted.WithLabelValues(string(summary.Action.Type)).Inc()
		}()

		// Track snapshot metrics
		bin, _ := next.ExportBinary()
		metrics.SnapshotSizeBytes.WithLabelValues("binary").Observe(float64(len(bin)))

		log.Printf("[GAMESERVER] Move committed: seq=%d action=%s player=%s next=%s (bin=%d B)",
			next.Sequence, summary.Action.Type, summary.Action.PlayerID, summary.NextPlayer, len(bin))
		return nil
	})

	// Setup HTTP server for Prometheus metrics (/metrics), Health (/healthz), and Profiling (/debug/pprof)
	mux := http.DefaultServeMux
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})
	status.Register(mux) // GET /role (live role for proxy & peer), GET /readyz (readiness probe)
	status.MarkReady()

	// Mirror the live role onto this pod's labels (arcade.io/role) when in Kubernetes.
	if labeler, lerr := fleet.InClusterLabeler(identity.Namespace, identity.PodName); lerr == nil {
		go labeler.Run(ctx, status, log.Printf)
	}

	// Synthetic self-play for load tests (Phase 2). Only the primary drives moves.
	if *autoplay > 0 && status.IsPrimary() {
		driver := &loadgen.Driver{Game: engine, Interval: *autoplay, Policy: loadgen.Policy{Endless: true}, MaxHookFailures: 100, Logf: log.Printf}
		go func() { _ = driver.Run(ctx) }()
		log.Printf("[LOADGEN] Self-play enabled: one move every %s on match=%s", *autoplay, *matchID)
	}

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: mux,
	}

	go func() {
		log.Printf("[GAMESERVER] Telemetry & pprof HTTP server listening on :%d", *httpPort)
		// -------------------------------------------------------------------------
		// Decoupled WebSocket Handler & Failure Detector Integration (Marcus & Team)
		// -------------------------------------------------------------------------
		wsHandler := NewGameServerWS(status, session)
		mux.Handle("/ws", wsHandler)

		// Automatically step down and close player sockets with 1012 on demotion
		status.OnChange(func(info fleet.RoleInfo) {
			if info.Role == fleet.RoleBackup {
				wsHandler.CloseAllDemoted()
			}
		})

		proxyAdminURL := os.Getenv("PROXY_ADMIN_URL")
		if proxyAdminURL == "" {
			proxyAdminURL = "http://arcade-proxy:9090"
		}

		det := detector.New(detector.Config{
			Identity:        identity,
			Status:          status,
			Store:           store,
			ProxyAdminURL:   proxyAdminURL,
			ProxyAdminToken: os.Getenv("ARCADE_ADMIN_TOKEN"),
			OnTakeover: func(epoch int64, newGame *uno.Game) {
				log.Printf("[TAKEOVER] Standby successfully promoted to Primary at Epoch=%d", epoch)
				if newGame != nil {
					session.SwapGame(newGame)
				}
				// Hook replication for subsequent primary moves
				rep := replication.NewReplicator(store, replication.ReplicatorConfig{
					MatchID: identity.MatchID,
					Epoch:   epoch,
					Format:  replication.Format(*snapshotFormat),
					OnFenced: func(fe *replication.FencedError) {
						status.Demote()
					},
				})
				session.RegisterHook(rep.Hook())
			},
			Logf: log.Printf,
		})

		if err := det.Start(ctx); err != nil {
			log.Printf("[DETECTOR] Warning: detector failed to start: %v", err)
		}
		defer det.Stop()
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[GAMESERVER] HTTP server error: %v", err)
		}
	}()

	// Graceful OS signal handling
	sig := <-sigCh
	log.Printf("[GAMESERVER] Caught termination signal %v. Shutting down.", sig)
	cancel()
	_ = server.Close()
}
