package detector

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/metrics"
	"github.com/CSC3104-ARCADE/pkg/replication"
	"github.com/CSC3104-ARCADE/pkg/uno"
)

// Config configures the failure detector.
type Config struct {
	Identity               fleet.Identity
	Status                 *fleet.Status
	Store                  *replication.Store
	HeartbeatInterval      time.Duration // Default 50ms
	HeartbeatTimeout       time.Duration // Default 200ms
	SnapshotSilenceTimeout time.Duration // Default 300ms
	ProxyAdminURL          string        // e.g. "http://arcade-proxy:9090"
	ProxyAdminToken        string
	OnTakeover             func(epoch int64, game *uno.Game)
	Logf                   func(format string, args ...any)
}

// Detector runs the UDP heartbeat transmitter, listener, and dual-validation monitor.
type Detector struct {
	cfg        Config
	udpConn    *net.UDPConn
	lastPeerHb atomic.Int64 // Unix nanoseconds
	peerEpoch  atomic.Int64
	peerSeq    atomic.Int64
	running    atomic.Bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
	logf       func(format string, args ...any)
}

// New creates an unstarted failure detector.
func New(cfg Config) *Detector {
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = 50 * time.Millisecond
	}
	if cfg.HeartbeatTimeout <= 0 {
		cfg.HeartbeatTimeout = 200 * time.Millisecond
	}
	if cfg.SnapshotSilenceTimeout <= 0 {
		cfg.SnapshotSilenceTimeout = 300 * time.Millisecond
	}
	logf := cfg.Logf
	if logf == nil {
		logf = log.Printf
	}
	return &Detector{
		cfg:    cfg,
		stopCh: make(chan struct{}),
		logf:   logf,
	}
}

// Start binds the UDP listener and launches sender, receiver, and monitor loops.
func (d *Detector) Start(ctx context.Context) error {
	if d.cfg.Identity.Standalone() {
		d.logf("[DETECTOR] Standalone mode: failure detector inactive")
		return nil
	}

	listenAddr, err := net.ResolveUDPAddr("udp", d.cfg.Identity.HeartbeatListenAddr())
	if err != nil {
		return fmt.Errorf("detector: resolve listen addr: %w", err)
	}

	conn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		return fmt.Errorf("detector: listen udp: %w", err)
	}
	d.udpConn = conn
	d.running.Store(true)

	// Initialize last peer heartbeat to now to prevent immediate false positive at boot
	d.lastPeerHb.Store(time.Now().UnixNano())

	d.wg.Add(3)
	go d.receiveLoop()
	go d.transmitLoop(ctx)
	go d.monitorLoop(ctx)

	d.logf("[DETECTOR] Listening for heartbeats on %s", d.cfg.Identity.HeartbeatListenAddr())
	return nil
}

// Stop gracefully shuts down all detector goroutines.
func (d *Detector) Stop() {
	if !d.running.Swap(false) {
		return
	}
	close(d.stopCh)
	if d.udpConn != nil {
		_ = d.udpConn.Close()
	}
	d.wg.Wait()
}

// receiveLoop continuously ingests incoming UDP heartbeats from the partner.
func (d *Detector) receiveLoop() {
	defer d.wg.Done()
	buf := make([]byte, 1024)

	for d.running.Load() {
		n, _, err := d.udpConn.ReadFrom(buf)
		if err != nil {
			if !d.running.Load() {
				return
			}
			continue
		}

		pkt, err := decodePacket(buf[:n])
		if err != nil {
			continue
		}

		if pkt.MatchID == d.cfg.Identity.MatchID {
			d.lastPeerHb.Store(time.Now().UnixNano())
			d.peerEpoch.Store(pkt.Epoch)
			d.peerSeq.Store(pkt.Sequence)
		}
	}
}

// transmitLoop broadcasts heartbeats to the peer at regular intervals.
func (d *Detector) transmitLoop(ctx context.Context) {
	defer d.wg.Done()
	ticker := time.NewTicker(d.cfg.HeartbeatInterval)
	defer ticker.Stop()

	peerAddrStr, err := d.cfg.Identity.PeerHeartbeatAddr()
	if err != nil {
		return
	}
	peerAddr, err := net.ResolveUDPAddr("udp", peerAddrStr)
	if err != nil {
		d.logf("[DETECTOR] Failed to resolve peer addr: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			info := d.cfg.Status.Info()
			pkt := HeartbeatPacket{
				MatchID:   d.cfg.Identity.MatchID,
				PodName:   d.cfg.Identity.PodName,
				Role:      string(info.Role),
				Epoch:     info.Epoch,
				Timestamp: time.Now().UnixNano(),
			}

			data, err := encodePacket(pkt)
			if err == nil {
				_, _ = d.udpConn.WriteToUDP(data, peerAddr)
			}
		}
	}
}

// monitorLoop executes the dual-validation algorithm when operating in backup mode.
func (d *Detector) monitorLoop(ctx context.Context) {
	defer d.wg.Done()
	checkInterval := d.cfg.HeartbeatInterval / 2
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-d.stopCh:
			return
		case <-ticker.C:
			// Only standby pods monitor and attempt promotion
			if d.cfg.Status.IsPrimary() {
				continue
			}

			d.evaluateHealth(ctx)
		}
	}
}

// evaluateHealth performs dual-validation: UDP flatline AND Redis snapshot silence.
func (d *Detector) evaluateHealth(ctx context.Context) {
	lastHb := time.Unix(0, d.lastPeerHb.Load())
	udpFlatline := time.Since(lastHb) > d.cfg.HeartbeatTimeout

	if !udpFlatline {
		return // Primary is actively answering pings
	}

	// Condition 1 Met: UDP Flatlined. Perform Dual-Validation against Redis.
	if d.cfg.Store == nil {
		d.logf("[DETECTOR] UDP flatline detected, but Redis store unavailable for validation")
		return
	}

	age, err := d.cfg.Store.SnapshotAge(ctx, d.cfg.Identity.MatchID)
	redisSilent := false

	if errors.Is(err, replication.ErrNoSnapshot) {
		// No moves ever committed; rely on heartbeat flatline
		redisSilent = true
	} else if err == nil && age > d.cfg.SnapshotSilenceTimeout {
		redisSilent = true
	}

	if udpFlatline && redisSilent {
		d.logf("[DETECTOR] Dual-validation triggered: UDP flatline (%v ago) AND Redis silence (%v). Executing unilateral promotion...",
			time.Since(lastHb).Round(time.Millisecond), age.Round(time.Millisecond))

		d.executePromotion(ctx)
	}
}

// executePromotion executes the electionless takeover protocol.
func (d *Detector) executePromotion(ctx context.Context) {
	// Record failover downtime for Sam's Prometheus histogram (Phase 2)
	lastHb := time.Unix(0, d.lastPeerHb.Load())
	downtimeMs := float64(time.Since(lastHb).Milliseconds())
	metrics.FailoverDowntime.Observe(downtimeMs)

	matchID := d.cfg.Identity.MatchID
	expectedEpoch := d.peerEpoch.Load()

	// 1. Atomic compare-and-swap takeover in Redis (fences stale primary)
	newEpoch, latestSnap, err := d.cfg.Store.TakeOver(ctx, matchID, expectedEpoch)
	if err != nil {
		d.logf("[DETECTOR] Promotion aborted (takeover failed): %v", err)
		d.lastPeerHb.Store(time.Now().UnixNano())
		return
	}

	d.logf("[DETECTOR] Redis takeover succeeded. Claimed Epoch=%d (previous observed=%d)", newEpoch, expectedEpoch)

	// 2. Restore active game engine state from snapshot
	var game *uno.Game
	if latestSnap != nil {
		game, err = latestSnap.Game()
		if err != nil {
			d.logf("[DETECTOR] Corrupt snapshot restored on promotion: %v", err)
			return
		}
		d.logf("[DETECTOR] Resuming match=%s from sequence=%d", matchID, latestSnap.Sequence)
	}

	// 3. Promote local status and pod labels
	d.cfg.Status.Promote(newEpoch)

	// 4. Invoke local takeover callback to hook the replicator
	if d.cfg.OnTakeover != nil {
		d.cfg.OnTakeover(newEpoch, game)
	}

	// 5. Notify Proxy of the new route so player WebSockets retarget instantly
	if d.cfg.ProxyAdminURL != "" {
		go d.notifyProxy(context.Background(), matchID, newEpoch)
	}
}

// notifyProxy informs the arcade-proxy of the new primary address.
// notifyProxy informs the arcade-proxy of the new primary address.
func (d *Detector) notifyProxy(ctx context.Context, matchID string, epoch int64) {
	var primaryAddr string

	// If running outside Kubernetes (e.g. local e2e tests), advertise 127.0.0.1
	if d.cfg.Identity.PeerHost == "127.0.0.1" || d.cfg.Identity.Standalone() {
		primaryAddr = fmt.Sprintf("127.0.0.1:%d", d.cfg.Identity.HTTPPort)
	} else {
		// In Kubernetes, advertise the headless Service FQDN
		primaryAddr = fmt.Sprintf("%s.%s.%s.svc.%s:%d",
			d.cfg.Identity.PodName,
			d.cfg.Identity.Service,
			d.cfg.Identity.Namespace,
			fleet.DefaultClusterDomain,
			d.cfg.Identity.HTTPPort,
		)
	}

	payload := map[string]any{
		"match_id":     matchID,
		"epoch":        epoch,
		"primary_addr": primaryAddr,
	}

	body, _ := json.Marshal(payload)
	url := fmt.Sprintf("%s/admin/route", d.cfg.ProxyAdminURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		d.logf("[DETECTOR] Failed to construct proxy notification: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if d.cfg.ProxyAdminToken != "" {
		req.Header.Set("Authorization", "Bearer "+d.cfg.ProxyAdminToken)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		d.logf("[DETECTOR] Warning: proxy route notification failed: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		d.logf("[DETECTOR] Proxy route successfully updated: match=%s epoch=%d primary=%s", matchID, epoch, primaryAddr)
	} else {
		d.logf("[DETECTOR] Proxy returned error status: %s", resp.Status)
	}
}
