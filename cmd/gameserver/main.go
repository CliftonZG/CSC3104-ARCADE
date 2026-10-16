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
	"syscall"
	"time"

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
	snapshotFormat := flag.String("snapshot-format", "json", "replicated snapshot encoding: json or binary")
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

	log.Printf("[GAMESERVER] Booting pod role=%s match=%s seed=%d http=:%d", *role, *matchID, *seed, *httpPort)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	newGame := func() (*uno.Game, error) {
		return uno.NewGameEngine(*matchID, []string{"player1", "player2", "player3", "player4"}, *seed)
	}

	var engine *uno.Game
	var err error
	switch {
	case *redisAddr == "":
		log.Printf("[GAMESERVER] Replication disabled (no -redis / REDIS_ADDR)")
		engine, err = newGame()
	case *role == "primary":
		var store *replication.Store
		if store, err = connectStore(context.Background(), *redisAddr); err == nil {
			engine, err = startPrimary(context.Background(), store, *matchID, replication.Format(*snapshotFormat), newGame,
				func(fe *replication.FencedError) {
					// A newer primary owns the match; this pod's state is no longer authoritative.
					log.Printf("[REPLICATION] %v. Stepping down.", fe)
					select {
					case sigCh <- syscall.SIGTERM:
					default:
					}
				})
		}
	default:
		// Standby: promotion (store.TakeOver) is driven by the failure detector.
		log.Printf("[GAMESERVER] Standby for match=%s; replication source redis=%s", *matchID, *redisAddr)
		engine, err = newGame()
	}
	if err != nil {
		log.Fatalf("[GAMESERVER] Failed to initialize engine: %v", err)
	}

	metrics.ActiveMatches.Inc()
	defer metrics.ActiveMatches.Dec()

	chk, _ := engine.Checksum()
	log.Printf("[GAMESERVER] State engine online. Initial sequence=%d SHA256=%s", engine.Sequence(), chk)

	// Register move hook for metrics instrumentation and replication
	engine.RegisterHook(func(prev, next *uno.State, summary uno.ActionSummary) error {
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

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *httpPort),
		Handler: mux,
	}

	go func() {
		log.Printf("[GAMESERVER] Telemetry & pprof HTTP server listening on :%d", *httpPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[GAMESERVER] HTTP server error: %v", err)
		}
	}()

	// Graceful OS signal handling
	sig := <-sigCh
	log.Printf("[GAMESERVER] Caught termination signal %v. Shutting down.", sig)
	_ = server.Close()
}
