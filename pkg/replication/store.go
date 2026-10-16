// Package replication streams authoritative Uno snapshots from the primary pod
// to Redis and fences stale primaries out with epoch (generation) tokens.
//
// Every match owns three Redis keys, hash-tagged on the match id so they land
// in the same Redis Cluster slot and can be touched by one Lua script:
//
//	<prefix>:match:{<id>}:epoch     current fencing epoch (integer counter)
//	<prefix>:match:{<id>}:snapshot  latest snapshot (hash: data, format, seq, epoch, ts_ms)
//	<prefix>:match:{<id>}:stream    capped stream of recent snapshots
//
// A pod becomes primary by claiming the next epoch (ClaimEpoch / TakeOver).
// Every snapshot write carries the writer's epoch and is checked atomically
// against the current epoch inside Redis, so a primary that was partitioned
// away and later wakes up is rejected with ErrFenced instead of overwriting
// the promoted backup's state.
package replication

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/redis/go-redis/v9"
)

// DefaultKeyPrefix namespaces all keys written by this package.
const DefaultKeyPrefix = "arcade"

// DefaultStreamMaxLen is the approximate number of snapshots retained per match stream.
const DefaultStreamMaxLen = 256

var (
	//go:embed scripts/commit.lua
	commitSrc    string
	commitScript = redis.NewScript(commitSrc)

	//go:embed scripts/claim.lua
	claimSrc    string
	claimScript = redis.NewScript(claimSrc)
)

// Format identifies how a snapshot payload is encoded.
type Format string

const (
	// FormatJSON is the canonical JSON encoding from uno.State.ExportSnapshot.
	FormatJSON Format = "json"
	// FormatBinary is the compact encoding from uno.State.ExportBinary.
	FormatBinary Format = "binary"
)

// Encode serializes st in format f.
func (f Format) Encode(st *uno.State) ([]byte, error) {
	switch f {
	case FormatJSON:
		return st.ExportSnapshot()
	case FormatBinary:
		return st.ExportBinary()
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFormat, f)
	}
}

// Decode restores and validates a state encoded in format f.
func (f Format) Decode(data []byte) (*uno.State, error) {
	switch f {
	case FormatJSON:
		return uno.RestoreFromSnapshot(data)
	case FormatBinary:
		return uno.RestoreFromBinary(data)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownFormat, f)
	}
}

// Snapshot is one replicated copy of a match state.
type Snapshot struct {
	MatchID  string
	Epoch    int64
	Sequence int64
	Format   Format
	Data     []byte
	// WrittenAt is the Redis server time at which the snapshot was committed.
	WrittenAt time.Time
}

// State decodes the snapshot payload into a validated uno.State.
func (s *Snapshot) State() (*uno.State, error) {
	return s.Format.Decode(s.Data)
}

// Game decodes the snapshot into a ready-to-play engine.
func (s *Snapshot) Game() (*uno.Game, error) {
	st, err := s.State()
	if err != nil {
		return nil, err
	}
	return uno.NewGameEngineFromState(st)
}

// Store is the Redis-backed snapshot store and fencing authority.
// It is safe for concurrent use.
type Store struct {
	rdb          redis.UniversalClient
	prefix       string
	streamMaxLen int64
}

// Option configures a Store.
type Option func(*Store)

// WithKeyPrefix overrides DefaultKeyPrefix.
func WithKeyPrefix(prefix string) Option {
	return func(s *Store) { s.prefix = prefix }
}

// WithStreamMaxLen sets the approximate snapshot stream length; 0 disables the stream.
func WithStreamMaxLen(n int64) Option {
	return func(s *Store) { s.streamMaxLen = n }
}

// NewStore wraps a go-redis client (single node, sentinel or cluster).
func NewStore(rdb redis.UniversalClient, opts ...Option) *Store {
	s := &Store{
		rdb:          rdb,
		prefix:       DefaultKeyPrefix,
		streamMaxLen: DefaultStreamMaxLen,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *Store) key(matchID, suffix string) string {
	return s.prefix + ":match:{" + matchID + "}:" + suffix
}

func (s *Store) epochKey(matchID string) string    { return s.key(matchID, "epoch") }
func (s *Store) snapshotKey(matchID string) string { return s.key(matchID, "snapshot") }
func (s *Store) streamKey(matchID string) string   { return s.key(matchID, "stream") }

// Write commits snap if snap.Epoch is the match's current epoch and
// snap.Sequence is newer than the stored snapshot. On success snap.WrittenAt
// is set to the commit time. Retrying an already-committed write is a no-op.
func (s *Store) Write(ctx context.Context, snap *Snapshot) error {
	keys := []string{s.epochKey(snap.MatchID), s.snapshotKey(snap.MatchID), s.streamKey(snap.MatchID)}
	res, err := commitScript.Run(ctx, s.rdb, keys,
		snap.Epoch, snap.Sequence, string(snap.Format), snap.Data, s.streamMaxLen).Slice()
	if err != nil {
		return fmt.Errorf("replication: commit snapshot: %w", err)
	}
	status, value, err := parseReply(res)
	if err != nil {
		return err
	}
	switch status {
	case "OK", "DUP":
		snap.WrittenAt = time.UnixMilli(value)
		return nil
	case "FENCED":
		return &FencedError{MatchID: snap.MatchID, Held: snap.Epoch, Current: value}
	case "STALE":
		return fmt.Errorf("%w: match %q seq %d, stored seq %d", ErrStaleSequence, snap.MatchID, snap.Sequence, value)
	default:
		return fmt.Errorf("replication: unexpected commit status %q", status)
	}
}

// ClaimEpoch atomically advances the match epoch from expected to expected+1
// and returns the new epoch. From that moment any writer holding an older
// epoch is fenced. If the epoch is no longer expected (another pod claimed
// first) it returns a *FencedError carrying the current epoch.
func (s *Store) ClaimEpoch(ctx context.Context, matchID string, expected int64) (int64, error) {
	res, err := claimScript.Run(ctx, s.rdb, []string{s.epochKey(matchID)}, expected).Slice()
	if err != nil {
		return 0, fmt.Errorf("replication: claim epoch: %w", err)
	}
	status, value, err := parseReply(res)
	if err != nil {
		return 0, err
	}
	switch status {
	case "OK":
		return value, nil
	case "FENCED":
		return 0, &FencedError{MatchID: matchID, Held: expected, Current: value}
	default:
		return 0, fmt.Errorf("replication: unexpected claim status %q", status)
	}
}

// CurrentEpoch returns the match's current epoch (0 if never claimed).
func (s *Store) CurrentEpoch(ctx context.Context, matchID string) (int64, error) {
	v, err := s.rdb.Get(ctx, s.epochKey(matchID)).Int64()
	if errors.Is(err, redis.Nil) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("replication: read epoch: %w", err)
	}
	return v, nil
}

// Latest returns the newest committed snapshot, or ErrNoSnapshot.
func (s *Store) Latest(ctx context.Context, matchID string) (*Snapshot, error) {
	vals, err := s.rdb.HMGet(ctx, s.snapshotKey(matchID), "data", "format", "seq", "epoch", "ts_ms").Result()
	if err != nil {
		return nil, fmt.Errorf("replication: read snapshot: %w", err)
	}
	if vals[0] == nil {
		return nil, fmt.Errorf("%w: %q", ErrNoSnapshot, matchID)
	}
	return snapshotFromFields(matchID, vals[0], vals[1], vals[2], vals[3], vals[4])
}

// TakeOver is the promotion step for a backup pod: it claims the next epoch
// (fencing the old primary) and only then reads the newest snapshot. Claiming
// first guarantees the snapshot cannot change afterwards, because the old
// primary can no longer commit. latest is nil if the match was never written.
//
// expectedEpoch should be the epoch the backup last saw the primary using
// (e.g. Snapshot.Epoch of the last snapshot it observed); if another pod has
// already promoted, TakeOver returns a *FencedError and the caller must not
// serve the match.
func (s *Store) TakeOver(ctx context.Context, matchID string, expectedEpoch int64) (epoch int64, latest *Snapshot, err error) {
	epoch, err = s.ClaimEpoch(ctx, matchID, expectedEpoch)
	if err != nil {
		return 0, nil, err
	}
	latest, err = s.Latest(ctx, matchID)
	if errors.Is(err, ErrNoSnapshot) {
		return epoch, nil, nil
	}
	if err != nil {
		return epoch, nil, err
	}
	return epoch, latest, nil
}

// SnapshotAge reports how long ago the newest snapshot was committed, measured
// entirely on the Redis server clock so pod clock skew cannot distort it.
// This is the "Redis snapshot silence" signal for failure detection.
func (s *Store) SnapshotAge(ctx context.Context, matchID string) (time.Duration, error) {
	pipe := s.rdb.Pipeline()
	now := pipe.Time(ctx)
	ts := pipe.HGet(ctx, s.snapshotKey(matchID), "ts_ms")
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("replication: read snapshot age: %w", err)
	}
	ms, err := ts.Int64()
	if errors.Is(err, redis.Nil) {
		return 0, fmt.Errorf("%w: %q", ErrNoSnapshot, matchID)
	}
	if err != nil {
		return 0, fmt.Errorf("replication: parse snapshot time: %w", err)
	}
	return now.Val().Sub(time.UnixMilli(ms)), nil
}

// Follow blocks, invoking fn for every snapshot appended to the match stream
// after the stream entry lastID ("$" for only new snapshots, "0" for all
// retained ones), until ctx is cancelled or fn returns an error. A backup pod
// can use it to keep a hot copy of the state and observe the primary's
// liveness without polling. It returns ctx.Err() on cancellation.
func (s *Store) Follow(ctx context.Context, matchID, lastID string, fn func(*Snapshot) error) error {
	stream := s.streamKey(matchID)
	for {
		res, err := s.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{stream, lastID},
			Block:   time.Second,
		}).Result()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, redis.Nil) {
			continue // block timed out with no new entries
		}
		if err != nil {
			return fmt.Errorf("replication: follow stream: %w", err)
		}
		for _, msg := range res[0].Messages {
			lastID = msg.ID
			snap, err := snapshotFromStream(matchID, msg)
			if err != nil {
				return err
			}
			if err := fn(snap); err != nil {
				return err
			}
		}
	}
}

func snapshotFromStream(matchID string, msg redis.XMessage) (*Snapshot, error) {
	// Stream IDs are "<ms>-<n>" where <ms> is the Redis server time of the XADD.
	msPart, _, _ := strings.Cut(msg.ID, "-")
	v := msg.Values
	return snapshotFromFields(matchID, v["data"], v["format"], v["seq"], v["epoch"], msPart)
}

func snapshotFromFields(matchID string, data, format, seq, epoch, tsMs interface{}) (*Snapshot, error) {
	snap := &Snapshot{
		MatchID: matchID,
		Format:  Format(asString(format)),
		Data:    []byte(asString(data)),
	}
	var err error
	if snap.Sequence, err = strconv.ParseInt(asString(seq), 10, 64); err != nil {
		return nil, fmt.Errorf("replication: corrupt snapshot seq: %w", err)
	}
	if snap.Epoch, err = strconv.ParseInt(asString(epoch), 10, 64); err != nil {
		return nil, fmt.Errorf("replication: corrupt snapshot epoch: %w", err)
	}
	ms, err := strconv.ParseInt(asString(tsMs), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("replication: corrupt snapshot timestamp: %w", err)
	}
	snap.WrittenAt = time.UnixMilli(ms)
	return snap, nil
}

func asString(v interface{}) string {
	s, _ := v.(string)
	return s
}

// parseReply decodes a {status, value} Lua reply. Redis returns Lua numbers
// as integers and Lua strings as strings, so value may be either.
func parseReply(res []interface{}) (string, int64, error) {
	if len(res) != 2 {
		return "", 0, fmt.Errorf("replication: malformed script reply %v", res)
	}
	status, ok := res[0].(string)
	if !ok {
		return "", 0, fmt.Errorf("replication: malformed script status %v", res[0])
	}
	switch v := res[1].(type) {
	case int64:
		return status, v, nil
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", 0, fmt.Errorf("replication: malformed script value %q: %w", v, err)
		}
		return status, n, nil
	default:
		return "", 0, fmt.Errorf("replication: malformed script value %v", res[1])
	}
}
