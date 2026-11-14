package detector

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

// The partner lookup runs beside the heartbeat sender and fills in the address
// without blocking it.
func TestResolvePeerLoopStoresAddress(t *testing.T) {
	d := &Detector{cfg: Config{HeartbeatInterval: 10 * time.Millisecond}, stopCh: make(chan struct{}), logf: func(string, ...any) {}}
	var addr atomic.Pointer[net.UDPAddr]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.resolvePeerLoop(ctx, "127.0.0.1:9000", &addr)

	deadline := time.Now().Add(2 * time.Second)
	for addr.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	got := addr.Load()
	if got == nil || got.Port != 9000 || !got.IP.Equal(net.IPv4(127, 0, 0, 1)) {
		t.Fatalf("resolved address = %v, want 127.0.0.1:9000", got)
	}
}
