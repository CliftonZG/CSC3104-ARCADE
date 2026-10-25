package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/redis/go-redis/v9"
)

// connectStore dials Redis and verifies it is reachable.
func connectStore(ctx context.Context, addr string) (*replication.Store, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, fmt.Errorf("redis %s unreachable: %w", addr, err)
	}
	return replication.NewStore(rdb), nil
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
