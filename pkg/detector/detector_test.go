package detector

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func getFreePort(t *testing.T) int {
	t.Helper()
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	return l.LocalAddr().(*net.UDPAddr).Port
}

func TestDualValidationTakeover(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	store := replication.NewStore(rdb)

	portA := getFreePort(t)
	portB := getFreePort(t)

	idA := fleet.Identity{
		PodName:           "arcade-gs-a-0",
		MatchID:           "match-000",
		InitialRole:       fleet.RolePrimary,
		HeartbeatPort:     portA,
		PeerHeartbeatPort: portB,
		PeerHost:          "127.0.0.1",
	}

	idB := fleet.Identity{
		PodName:           "arcade-gs-b-0",
		MatchID:           "match-000",
		InitialRole:       fleet.RoleBackup,
		HeartbeatPort:     portB,
		PeerHeartbeatPort: portA,
		PeerHost:          "127.0.0.1",
	}

	statusA := fleet.NewStatus(idA, fleet.RolePrimary)
	statusB := fleet.NewStatus(idB, fleet.RoleBackup)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initial epoch claimed by A
	epochA, err := store.ClaimEpoch(ctx, "match-000", 0)
	if err != nil {
		t.Fatalf("ClaimEpoch: %v", err)
	}
	statusA.SetEpoch(epochA)

	detA := New(Config{
		Identity:          idA,
		Status:            statusA,
		Store:             store,
		HeartbeatInterval: 20 * time.Millisecond,
	})
	if err := detA.Start(ctx); err != nil {
		t.Fatalf("detA Start: %v", err)
	}
	defer detA.Stop()

	promoted := make(chan int64, 1)
	detB := New(Config{
		Identity:               idB,
		Status:                 statusB,
		Store:                  store,
		HeartbeatInterval:      20 * time.Millisecond,
		HeartbeatTimeout:       60 * time.Millisecond,
		SnapshotSilenceTimeout: 60 * time.Millisecond,
		OnTakeover: func(epoch int64, game *uno.Game) {
			promoted <- epoch
		},
	})
	if err := detB.Start(ctx); err != nil {
		t.Fatalf("detB Start: %v", err)
	}
	defer detB.Stop()

	// Verify heartbeats keep B in standby mode
	time.Sleep(100 * time.Millisecond)
	if statusB.IsPrimary() {
		t.Fatal("Backup pod should not promote while primary is healthy")
	}

	// Simulate Primary crash by stopping A's detector
	detA.Stop()

	select {
	case newEpoch := <-promoted:
		if newEpoch != epochA+1 {
			t.Fatalf("Promoted with epoch %d, want %d", newEpoch, epochA+1)
		}
		if !statusB.IsPrimary() {
			t.Fatal("statusB should reflect Primary role")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Dual-validation takeover timed out")
	}
}
