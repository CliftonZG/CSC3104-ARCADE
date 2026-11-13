package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/redis/go-redis/v9"
)

// redisConfig selects a single Redis node (Addr) or a Sentinel-managed
// primary/replica group (Sentinels + MasterName). MinAcks > 0 makes every
// claim and commit wait for that many replicas (see replication.WithReplicaAcks).
type redisConfig struct {
	Addr         string
	Sentinels    []string
	MasterName   string
	MinAcks      int
	AckTimeout   time.Duration
	WriteTimeout time.Duration
}

func (c redisConfig) enabled() bool { return c.Addr != "" || len(c.Sentinels) > 0 }

func (c redisConfig) String() string {
	if len(c.Sentinels) > 0 {
		return fmt.Sprintf("sentinel master %q via %s (replica acks %d)", c.MasterName, strings.Join(c.Sentinels, ","), c.MinAcks)
	}
	return fmt.Sprintf("%s (replica acks %d)", c.Addr, c.MinAcks)
}

// splitAddrs parses a comma-separated host:port list.
func splitAddrs(s string) []string {
	var out []string
	for _, a := range strings.Split(s, ",") {
		if a = strings.TrimSpace(a); a != "" {
			out = append(out, a)
		}
	}
	return out
}

// connectStore dials Redis and verifies it is reachable.
func connectStore(ctx context.Context, cfg redisConfig) (*replication.Store, error) {
	opts := &redis.UniversalOptions{Addrs: []string{cfg.Addr}}
	if len(cfg.Sentinels) > 0 {
		// A Sentinel client asks the sentinels for the current primary and
		// reconnects to the new one after a Redis failover.
		opts = &redis.UniversalOptions{Addrs: cfg.Sentinels, MasterName: cfg.MasterName}
	}
	rdb := redis.NewUniversalClient(opts)
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis %s unreachable: %w", cfg, err)
	}
	writeTimeout := cfg.WriteTimeout
	if writeTimeout <= 0 && len(cfg.Sentinels) > 0 {
		writeTimeout = 3 * time.Second // ride out a Sentinel failover (~1-2s)
	}
	return replication.NewStore(rdb, replication.WithReplicaAcks(cfg.MinAcks, cfg.AckTimeout),
		replication.WithWriteTimeout(writeTimeout)), nil
}

// startPrimary claims the next fencing epoch for the match (revoking any
// previous primary's write access), resumes from the latest snapshot if one
// exists or otherwise starts a fresh game, and registers the replication hook
// so every valid move is committed to Redis.
func startPrimary(ctx context.Context, store *replication.Store, matchID string, format replication.Format,
	newGame func() (*uno.Game, error), onFenced func(*replication.FencedError)) (*uno.Game, error) {

	observed, err := store.CurrentEpoch(ctx, matchID)
	if err != nil {
		return nil, err
	}
	epoch, latest, err := store.TakeOver(ctx, matchID, observed)
	if err != nil {
		return nil, fmt.Errorf("claim epoch: %w", err)
	}

	var engine *uno.Game
	if latest != nil {
		engine, err = latest.Game()
		log.Printf("[REPLICATION] Resuming match=%s from snapshot seq=%d (written by epoch %d)", matchID, latest.Sequence, latest.Epoch)
	} else {
		engine, err = newGame()
	}
	if err != nil {
		return nil, err
	}

	rep := replication.NewReplicator(store, replication.ReplicatorConfig{
		MatchID:  matchID,
		Epoch:    epoch,
		Format:   format,
		OnFenced: onFenced,
	})
	engine.RegisterHook(rep.Hook())

	if latest == nil {
		if err := rep.Publish(ctx, engine.State()); err != nil {
			return nil, fmt.Errorf("publish initial snapshot: %w", err)
		}
	}
	log.Printf("[REPLICATION] Primary holds epoch=%d for match=%s (format=%s)", epoch, matchID, format)
	return engine, nil
}
