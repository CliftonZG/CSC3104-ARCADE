package detector

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TestSensitivityBenchmark evaluates failover detection time across 20ms vs 50ms intervals.
func TestSensitivityBenchmark(t *testing.T) {
	intervals := []struct {
		name       string
		hbInterval time.Duration
		hbTimeout  time.Duration
	}{
		{"Aggressive_20ms", 20 * time.Millisecond, 60 * time.Millisecond},
		{"Standard_50ms", 50 * time.Millisecond, 150 * time.Millisecond},
	}

	for _, tc := range intervals {
		t.Run(tc.name, func(t *testing.T) {
			mr := miniredis.RunT(t)
			rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			store := replication.NewStore(rdb)

			portA := getFreePort(t)
			portB := getFreePort(t)

			idA := fleet.Identity{
				PodName: "arcade-gs-a-0", MatchID: "bench-match", InitialRole: fleet.RolePrimary,
				HeartbeatPort: portA, PeerHeartbeatPort: portB, PeerHost: "127.0.0.1",
			}
			idB := fleet.Identity{
				PodName: "arcade-gs-b-0", MatchID: "bench-match", InitialRole: fleet.RoleBackup,
				HeartbeatPort: portB, PeerHeartbeatPort: portA, PeerHost: "127.0.0.1",
			}

			statusA := fleet.NewStatus(idA, fleet.RolePrimary)
			statusB := fleet.NewStatus(idB, fleet.RoleBackup)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			epochA, _ := store.ClaimEpoch(ctx, "bench-match", 0)
			statusA.SetEpoch(epochA)

			detA := New(Config{
				Identity: idA, Status: statusA, Store: store,
				HeartbeatInterval: tc.hbInterval, HeartbeatTimeout: tc.hbTimeout,
			})
			_ = detA.Start(ctx)

			takeoverCh := make(chan time.Duration, 1)
			detB := New(Config{
				Identity: idB, Status: statusB, Store: store,
				HeartbeatInterval: tc.hbInterval, HeartbeatTimeout: tc.hbTimeout,
				SnapshotSilenceTimeout: tc.hbTimeout,
				OnTakeover: func(epoch int64, game *uno.Game) {
					// Takeover callback
				},
			})
			_ = detB.Start(ctx)

			// Wait for heartbeats to stabilize
			time.Sleep(tc.hbInterval * 3)

			// Kill Primary and measure latency until Standby detects and takes over
			crashTime := time.Now()
			detA.Stop()

			go func() {
				for {
					if statusB.IsPrimary() {
						takeoverCh <- time.Since(crashTime)
						return
					}
					time.Sleep(2 * time.Millisecond)
				}
			}()

			select {
			case latency := <-takeoverCh:
				t.Logf("[%s] Failure Detection & Takeover completed in: %v (Timeout configured: %v)",
					tc.name, latency, tc.hbTimeout)
				if latency > tc.hbTimeout*3 {
					t.Errorf("Latency %v exceeded 3x timeout threshold %v", latency, tc.hbTimeout)
				}
			case <-time.After(1 * time.Second):
				t.Fatal("Timeout waiting for takeover")
			}
			detB.Stop()
		})
	}
}

// TestFalsePositiveSuppression verifies that dual-validation does NOT spuriously trigger
// when UDP packets are dropped but Redis snapshot commits are still active.
func TestFalsePositiveSuppression(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := replication.NewStore(rdb)

	portA := getFreePort(t)
	portB := getFreePort(t)

	idA := fleet.Identity{
		PodName: "arcade-gs-a-0", MatchID: "fp-match", InitialRole: fleet.RolePrimary,
		HeartbeatPort: portA, PeerHeartbeatPort: portB, PeerHost: "127.0.0.1",
	}
	idB := fleet.Identity{
		PodName: "arcade-gs-b-0", MatchID: "fp-match", InitialRole: fleet.RoleBackup,
		HeartbeatPort: portB, PeerHeartbeatPort: portA, PeerHost: "127.0.0.1",
	}

	statusA := fleet.NewStatus(idA, fleet.RolePrimary)
	statusB := fleet.NewStatus(idB, fleet.RoleBackup)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	epochA, _ := store.ClaimEpoch(ctx, "fp-match", 0)
	statusA.SetEpoch(epochA)

	var takeovers atomic.Int32
	detB := New(Config{
		Identity: idB, Status: statusB, Store: store,
		HeartbeatInterval:      20 * time.Millisecond,
		HeartbeatTimeout:       50 * time.Millisecond,
		SnapshotSilenceTimeout: 200 * time.Millisecond, // Redis silence window is wider
		OnTakeover: func(epoch int64, game *uno.Game) {
			takeovers.Add(1)
		},
	})
	_ = detB.Start(ctx)
	defer detB.Stop()

	// Primary does NOT send UDP heartbeats (simulating UDP partition / transient packet drop),
	// but actively commits moves to Redis.
	for i := int64(1); i <= 5; i++ {
		_ = store.Write(ctx, &replication.Snapshot{
			MatchID:  "fp-match",
			Epoch:    epochA,
			Sequence: i,
			Format:   replication.FormatJSON,
			Data:     []byte("{}"),
		})
		time.Sleep(30 * time.Millisecond)
	}

	// Because Redis was active within SnapshotSilenceTimeout, Standby must NOT have promoted
	if takeovers.Load() > 0 || statusB.IsPrimary() {
		t.Fatalf("False positive detected: Standby took over while primary was still committing to Redis")
	}
	t.Log("PASS: Dual-validation suppressed false takeover during UDP outage while Redis commits were active.")
}
