package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/CSC3104-ARCADE/pkg/proxy"
)

func TestParseRoute(t *testing.T) {
	good := map[string]proxy.Update{
		"m1=3@10.0.0.1:8080":         {MatchID: "m1", Epoch: 3, PrimaryAddr: "10.0.0.1:8080"},
		"room-9=0@game-0.g:81":       {MatchID: "room-9", Epoch: 0, PrimaryAddr: "game-0.g:81"},
		"x=18446744073709551615@h:1": {MatchID: "x", Epoch: 18446744073709551615, PrimaryAddr: "h:1"},
	}
	for in, want := range good {
		got, err := parseRoute(in)
		if err != nil || got != want {
			t.Errorf("parseRoute(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "m1", "m1=3", "m1=@h:1", "m1=-1@h:1", "m1=x@h:1", "=1@h:1", "m1=1@"} {
		u, err := parseRoute(in)
		if err == nil {
			// Syntactically parsed; the table must still reject it.
			if _, err := proxy.NewTable().Apply(u); err == nil {
				t.Errorf("parseRoute(%q) and Apply both accepted %+v", in, u)
			}
		}
	}
}

func TestParseFlags(t *testing.T) {
	var sink bytes.Buffer
	o, err := parseFlags([]string{"-listen", ":1", "-origin", "a.example", "-origin", "*.b.example",
		"-route", "m=1@h:2", "-max-outage", "7s", "-pending-messages", "9"}, &sink)
	if err != nil {
		t.Fatal(err)
	}
	if o.listen != ":1" || len(o.origins) != 2 || len(o.routes) != 1 || o.cfg.MaxOutage != 7*time.Second || o.cfg.PendingMaxMessages != 9 {
		t.Fatalf("unexpected options: %+v", o)
	}
	if _, err := parseFlags([]string{"-nope"}, &sink); err == nil {
		t.Error("unknown flag accepted")
	}
	if _, err := parseFlags([]string{"stray"}, &sink); err == nil {
		t.Error("positional argument accepted")
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().String()
}

func TestRunServesAndShutsDown(t *testing.T) {
	listen, admin := freeAddr(t), freeAddr(t)
	ctx, cancel := context.WithCancel(context.Background())
	var logs bytes.Buffer
	done := make(chan error, 1)
	go func() {
		done <- run(ctx, []string{"-listen", listen, "-admin", admin, "-admin-token", "tok",
			"-route", "m1=1@127.0.0.1:9", "-log-level", "warn"}, &logs)
	}()

	get := func(url, token string) (int, string) {
		req, _ := http.NewRequest("GET", url, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		for i := 0; i < 100; i++ {
			resp, err := http.DefaultClient.Do(req)
			if err == nil {
				var b bytes.Buffer
				_, _ = b.ReadFrom(resp.Body)
				_ = resp.Body.Close()
				return resp.StatusCode, b.String()
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("%s never came up", url)
		return 0, ""
	}

	if code, _ := get("http://"+listen+"/healthz", ""); code != 200 {
		t.Fatalf("client /healthz = %d", code)
	}
	if code, _ := get("http://"+admin+"/admin/routes", ""); code != 401 {
		t.Fatalf("admin without token = %d, want 401", code)
	}
	code, body := get("http://"+admin+"/admin/routes", "tok")
	if code != 200 || !strings.Contains(body, `"m1":{"epoch":1,"primary_addr":"127.0.0.1:9"}`) {
		t.Fatalf("admin routes = %d %s (initial -route not applied)", code, body)
	}
	if code, _ := get(fmt.Sprintf("http://%s/ws?match=m1", listen), ""); code != http.StatusUpgradeRequired {
		t.Fatalf("/ws without upgrade = %d, want 426", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after the context was cancelled")
	}
}

func TestRunRejectsBadConfiguration(t *testing.T) {
	var sink bytes.Buffer
	for _, args := range [][]string{
		{"-default-upstream", "not-an-address"},
		{"-route", "garbage"},
		{"-route", "bad id=1@h:1"},
		{"-log-level", "loud"},
		{"-discover"}, // no sets configured
		{"-discover-pods", "not-an-address"},
	} {
		if err := run(context.Background(), args, &sink); err == nil {
			t.Errorf("run(%v) succeeded, want an error", args)
		}
	}
}

func TestCompatibilityFlags(t *testing.T) {
	var sink bytes.Buffer
	o, err := parseFlags([]string{"-port=8123", "-primary", "game-0:9001"}, &sink)
	if err != nil {
		t.Fatal(err)
	}
	if o.listen != ":8123" || o.defaultUpstream != "game-0:9001" {
		t.Fatalf("-port/-primary not applied: listen=%q default-upstream=%q", o.listen, o.defaultUpstream)
	}
	o, err = parseFlags([]string{"-default-upstream", "x:1"}, &sink)
	if err != nil || o.defaultUpstream != "x:1" || o.listen != ":8080" {
		t.Fatalf("defaults broken: %+v, %v", o, err)
	}
}

func TestDiscoveryFlags(t *testing.T) {
	var sink bytes.Buffer

	// Off by default.
	o, err := parseFlags(nil, &sink)
	if err != nil {
		t.Fatal(err)
	}
	if cfg, err := o.discoveryConfig(); cfg != nil || err != nil {
		t.Fatalf("discovery should be off by default, got %+v, %v", cfg, err)
	}

	// The Kubernetes layout, with the environment the manifest provides.
	t.Setenv("GAMESERVER_SETS", "arcade-gs-a,arcade-gs-b")
	t.Setenv("GAMESERVER_SERVICE", "arcade-gs")
	t.Setenv("GAMESERVER_HTTP_PORT", "8081")
	t.Setenv("POD_NAMESPACE", "arcade")
	o, err = parseFlags([]string{"-discover"}, &sink)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := o.discoveryConfig()
	if err != nil || cfg == nil {
		t.Fatalf("discoveryConfig = %+v, %v", cfg, err)
	}
	got := cfg.Targets("match-003")
	want := []string{
		"arcade-gs-a-3.arcade-gs.arcade.svc.cluster.local:8081",
		"arcade-gs-b-3.arcade-gs.arcade.svc.cluster.local:8081",
	}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("targets = %v, want %v", got, want)
	}

	// A flag beats the environment.
	o, err = parseFlags([]string{"-discover", "-discover-namespace", "other", "-discover-port", "9000"}, &sink)
	if err != nil {
		t.Fatal(err)
	}
	cfg, _ = o.discoveryConfig()
	if got := cfg.Targets("match-0"); got[0] != "arcade-gs-a-0.arcade-gs.other.svc.cluster.local:9000" {
		t.Fatalf("flags did not override the environment: %v", got)
	}

	// Static candidates for local runs apply to every match.
	o, err = parseFlags([]string{"-discover-pods", "127.0.0.1:9001", "-discover-pods", "127.0.0.1:9002"}, &sink)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err = o.discoveryConfig()
	if err != nil || cfg == nil {
		t.Fatalf("discoveryConfig = %+v, %v", cfg, err)
	}
	if got := cfg.Targets("anything"); len(got) != 2 {
		t.Fatalf("static targets = %v", got)
	}
}
