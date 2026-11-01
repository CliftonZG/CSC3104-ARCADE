package replication

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CSC3104-ARCADE/pkg/metrics"
	"github.com/CSC3104-ARCADE/pkg/uno"
)

// DefaultWriteTimeout bounds a single snapshot commit issued from the move hook.
const DefaultWriteTimeout = 500 * time.Millisecond

// ReplicatorConfig configures the primary-side snapshot publisher.
type ReplicatorConfig struct {
	MatchID string
	// Epoch is the fencing token this pod obtained from ClaimEpoch or TakeOver.
	Epoch int64
	// Format defaults to FormatJSON.
	Format Format
	// WriteTimeout bounds each commit including retries; it defaults to the
	// Store's WithWriteTimeout, else DefaultWriteTimeout.
	WriteTimeout time.Duration
	// OnFenced is called once, the first time Redis rejects this pod's epoch.
	// The pod must stop serving the match: the engine has already applied the
	// rejected move in memory, so its state has diverged from the durable one.
	OnFenced func(*FencedError)
}

// Replicator publishes every committed move of the primary's engine to Redis
// under its fencing epoch.
type Replicator struct {
	store     *Store
	cfg       ReplicatorConfig
	fenced    atomic.Pointer[FencedError]
	fenceOnce sync.Once
}

// NewReplicator creates a publisher for one match on the primary pod.
func NewReplicator(store *Store, cfg ReplicatorConfig) *Replicator {
	if cfg.Format == "" {
		cfg.Format = FormatJSON
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = store.writeTimeout
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = DefaultWriteTimeout
	}
	return &Replicator{store: store, cfg: cfg}
}

// Epoch returns the fencing token this replicator writes with.
func (r *Replicator) Epoch() int64 { return r.cfg.Epoch }

// Fenced reports whether this pod has lost write access to the match.
func (r *Replicator) Fenced() bool { return r.fenced.Load() != nil }

// Hook returns a uno.MoveHook that synchronously commits the post-move state
// to Redis. Register it with Game.RegisterHook; a returned error surfaces from
// Game.ApplyAction wrapped in uno.ErrHookFailed.
func (r *Replicator) Hook() uno.MoveHook {
	return func(_, next *uno.State, _ uno.ActionSummary) error {
		return r.Publish(context.Background(), next)
	}
}

// Publish commits st to Redis under this pod's epoch. Call it once after
// becoming primary to persist the starting state; the hook covers every move
// after that. It gives up after WriteTimeout. Once fenced, Publish fails fast
// without contacting Redis.
func (r *Replicator) Publish(ctx context.Context, st *uno.State) error {
	if fe := r.fenced.Load(); fe != nil {
		return fe
	}
	ctx, cancel := context.WithTimeout(ctx, r.cfg.WriteTimeout)
	defer cancel()

	start := time.Now()
	data, err := r.cfg.Format.Encode(st)
	if err != nil {
		return err
	}
	snap := &Snapshot{
		MatchID:  r.cfg.MatchID,
		Epoch:    r.cfg.Epoch,
		Sequence: st.Sequence,
		Format:   r.cfg.Format,
		Data:     data,
	}
	// Transient failures (Redis failing over, a replica not yet caught up) are
	// retried until ctx expires; a repeated write is idempotent (DUP). Fencing
	// and stale sequences are final.
	var fe *FencedError
	for backoff := 25 * time.Millisecond; ; {
		err = r.store.Write(ctx, snap)
		if err == nil || errors.As(err, &fe) || errors.Is(err, ErrStaleSequence) {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w (gave up after %s)", err, time.Since(start).Round(time.Millisecond))
		case <-time.After(backoff):
		}
		if backoff < 400*time.Millisecond {
			backoff *= 2
		}
	}

	if fe != nil {
		r.fenced.Store(fe)
		r.fenceOnce.Do(func() {
			if r.cfg.OnFenced != nil {
				r.cfg.OnFenced(fe)
			}
		})
		return fe
	}
	if err != nil {
		return err
	}

	elapsedMs := float64(time.Since(start).Microseconds()) / 1000
	metrics.SnapshotSyncDuration.WithLabelValues(string(r.cfg.Format)).Observe(elapsedMs)
	return nil
}
