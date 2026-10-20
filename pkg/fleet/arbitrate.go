package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ProbePeer asks the partner pod for its current role via GET /role.
func ProbePeer(ctx context.Context, client *http.Client, id Identity) (RoleInfo, error) {
	addr, err := id.PeerHTTPAddr()
	if err != nil {
		return RoleInfo{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/role", nil)
	if err != nil {
		return RoleInfo{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return RoleInfo{}, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return RoleInfo{}, fmt.Errorf("fleet: peer %s /role returned %s", addr, resp.Status)
	}
	var info RoleInfo
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&info); err != nil {
		return RoleInfo{}, fmt.Errorf("fleet: decode peer role: %w", err)
	}
	return info, nil
}

// ArbitrateConfig tunes startup role arbitration.
type ArbitrateConfig struct {
	Client   *http.Client  // default: 1s timeout client
	Attempts int           // default 3
	Backoff  time.Duration // default 300ms between attempts
	Logf     func(format string, args ...any)
}

// Decision is the outcome of Arbitrate.
type Decision struct {
	Role   Role
	Reason string
	Peer   *RoleInfo // nil when the peer could not be reached
}

// Arbitrate decides the role this pod should actually start in.
//
// The manifest's ROLE is only the *initial* assignment: after a failover the
// standby (pod b-i) is the primary. When the crashed pod a-i is restarted by
// Kubernetes it still has ROLE=primary in its spec. Taking over again would
// claim a new fencing epoch and fence the healthy promoted pod, causing a
// needless second failover. So before acting as primary we ask the partner:
//
//   - partner says it is primary for our match -> we start as backup;
//   - partner is unreachable or is a backup     -> we keep our initial role.
//
// A pod whose initial role is backup never self-promotes here; promotion is
// the failure detector's job. This check is an optimisation for availability:
// correctness under races is still guaranteed by the Redis epoch fencing.
func Arbitrate(ctx context.Context, id Identity, cfg ArbitrateConfig) Decision {
	if id.InitialRole != RolePrimary {
		return Decision{Role: id.InitialRole, Reason: "initial role is backup; promotion is left to the failure detector"}
	}
	if id.Standalone() {
		return Decision{Role: RolePrimary, Reason: "standalone (no peer configured)"}
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: time.Second}
	}
	if cfg.Attempts <= 0 {
		cfg.Attempts = 3
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 300 * time.Millisecond
	}
	logf := cfg.Logf
	if logf == nil {
		logf = func(string, ...any) {}
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.Attempts; attempt++ {
		info, err := ProbePeer(ctx, cfg.Client, id)
		if err == nil {
			if info.Role == RolePrimary && info.MatchID == id.MatchID {
				return Decision{
					Role:   RoleBackup,
					Reason: fmt.Sprintf("peer %s already primary for %s at epoch %d", info.Pod, info.MatchID, info.Epoch),
					Peer:   &info,
				}
			}
			return Decision{Role: RolePrimary, Reason: fmt.Sprintf("peer %s is %s", info.Pod, info.Role), Peer: &info}
		}
		lastErr = err
		logf("[FLEET] peer probe %d/%d failed: %v", attempt, cfg.Attempts, err)
		if attempt < cfg.Attempts {
			select {
			case <-ctx.Done():
				return Decision{Role: RolePrimary, Reason: "context cancelled while probing peer"}
			case <-time.After(cfg.Backoff):
			}
		}
	}
	reason := "peer unreachable"
	if lastErr != nil && !errors.Is(lastErr, context.Canceled) {
		reason = fmt.Sprintf("peer unreachable (%v)", lastErr)
	}
	return Decision{Role: RolePrimary, Reason: reason}
}
