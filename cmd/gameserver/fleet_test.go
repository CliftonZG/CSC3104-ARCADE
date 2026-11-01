package main

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestConnectStoreWithRetryWaitsForRedis(t *testing.T) {
	// Reserve a port and start Redis on it only after a delay, like a fresh
	// Kubernetes deployment where the gameserver boots before Redis is ready.
	mr := miniredis.NewMiniRedis()
	addr := "127.0.0.1:" + freePort(t)
	go func() {
		time.Sleep(600 * time.Millisecond)
		_ = mr.StartAddr(addr)
	}()
	t.Cleanup(mr.Close)

	store, err := connectStoreWithRetry(context.Background(), redisConfig{Addr: addr}, 10*time.Second)
	if err != nil || store == nil {
		t.Fatalf("connectStoreWithRetry: %v", err)
	}
	if _, err := connectStoreWithRetry(context.Background(), redisConfig{Addr: "127.0.0.1:" + freePort(t)}, 300*time.Millisecond); err == nil {
		t.Fatal("expected error when Redis never comes up")
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	_, port, _ := net.SplitHostPort(l.Addr().String())
	return port
}
