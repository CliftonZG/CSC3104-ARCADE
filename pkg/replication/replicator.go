package replication

import (
	"context"
	"errors"
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
	// WriteTimeout defaults to DefaultWriteTimeout.
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
		ctx, cancel := context.WithTimeout(context.Background(), r.cfg.WriteTimeout)
		defer cancel()
		return r.Publish(ctx, next)
	}
}

// Publish commits st to Redis under this pod's epoch. Call it once after
// becoming primary to persist the starting state; the hook covers every move
// after that. Once fenced, Publish fails fast without contacting Redis.
func (r *Replicator) Publish(ctx context.Context, st *uno.State) error {
	if fe := r.fenced.Load(); fe != nil {
		return fe
	}

	start := time.Now()
	data, err := r.cfg.Format.Encode(st)
	if err != nil {
		return err
	}
	err = r.store.Write(ctx, &Snapshot{
		MatchID:  r.cfg.MatchID,
		Epoch:    r.cfg.Epoch,
		Sequence: st.Sequence,
		Format:   r.cfg.Format,
		Data:     data,
	})

	var fe *FencedError
	if errors.As(err, &fe) {
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
