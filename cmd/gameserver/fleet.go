package main

import (
	"context"
	"log"
	"time"

	"github.com/CSC3104-ARCADE/pkg/replication"
)

// connectStoreWithRetry keeps trying to reach Redis for up to wait before
// giving up. In Kubernetes all pods of a fresh deployment start at once and
// Redis is usually not ready for the first few seconds; exiting immediately
// would push the primaries into CrashLoopBackOff, whose exponential back-off
// then delays every later restart (including real failure recovery).
func connectStoreWithRetry(ctx context.Context, cfg redisConfig, wait time.Duration) (*replication.Store, error) {
	deadline := time.Now().Add(wait)
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		store, err := connectStore(ctx, cfg)
		if err == nil {
			return store, nil
		}
		if time.Now().Add(backoff).After(deadline) {
			return nil, err
		}
		log.Printf("[GAMESERVER] Redis not reachable yet (attempt %d): %v; retrying in %s", attempt, err, backoff)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 4*time.Second {
			backoff *= 2
		}
	}
}
