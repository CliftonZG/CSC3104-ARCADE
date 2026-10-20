package fleet

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

func envMap(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestResolveStatefulSetPod(t *testing.T) {
	id, err := Resolve(envMap(map[string]string{
		"POD_NAME":          "arcade-gs-a-7",
		"POD_NAMESPACE":     "arcade",
		"ROLE":              "primary",
		"PEER_STATEFULSET":  "arcade-gs-b",
		"GOVERNING_SERVICE": "arcade-gs",
	}), Defaults{})
	if err != nil {
		t.Fatal(err)
	}
	if id.StatefulSet != "arcade-gs-a" || id.Ordinal != 7 {
		t.Fatalf("parsed %q/%d", id.StatefulSet, id.Ordinal)
	}
	if id.MatchID != "match-007" {
		t.Fatalf("match = %q", id.MatchID)
	}
	if id.PeerName != "arcade-gs-b-7" {
		t.Fatalf("peer name = %q", id.PeerName)
	}
	if want := "arcade-gs-b-7.arcade-gs.arcade.svc.cluster.local"; id.PeerHost != want {
		t.Fatalf("peer host = %q, want %q", id.PeerHost, want)
	}
	hb, err := id.PeerHeartbeatAddr()
	if err != nil || hb != "arcade-gs-b-7.arcade-gs.arcade.svc.cluster.local:9000" {
		t.Fatalf("heartbeat addr = %q, %v", hb, err)
	}
	if id.InitialRole != RolePrimary || id.Standalone() {
		t.Fatalf("role=%s standalone=%v", id.InitialRole, id.Standalone())
	}
}

func TestResolveBackupAndOverrides(t *testing.T) {
	id, err := Resolve(envMap(map[string]string{
		"POD_NAME":            "arcade-gs-b-0",
		"ROLE":                "standby",
		"PEER_STATEFULSET":    "arcade-gs-a",
		"GOVERNING_SERVICE":   "arcade-gs",
		"MATCH_PREFIX":        "uno",
		"PEER_HOST":           "127.0.0.1",
		"PEER_HTTP_PORT":      "18081",
		"PEER_HEARTBEAT_PORT": "19000",
	}), Defaults{HTTPPort: 8082})
	if err != nil {
		t.Fatal(err)
	}
	if id.InitialRole != RoleBackup || id.MatchID != "uno-000" || id.HTTPPort != 8082 {
		t.Fatalf("got %+v", id)
	}
	if a, _ := id.PeerHTTPAddr(); a != "127.0.0.1:18081" {
		t.Fatalf("peer http = %q", a)
	}
	if a, _ := id.PeerHeartbeatAddr(); a != "127.0.0.1:19000" {
		t.Fatalf("peer hb = %q", a)
	}
}

func TestResolveStandaloneUsesFlags(t *testing.T) {
	id, err := Resolve(envMap(nil), Defaults{Role: "backup", MatchID: "match-demo"})
	if err != nil {
		t.Fatal(err)
	}
	if !id.Standalone() || id.MatchID != "match-demo" || id.InitialRole != RoleBackup || id.Ordinal != -1 {
		t.Fatalf("got %+v", id)
	}
	if _, err := id.PeerHeartbeatAddr(); err != ErrNoPeer {
		t.Fatalf("want ErrNoPeer, got %v", err)
	}
}

func TestResolveExplicitMatchIDWins(t *testing.T) {
	id, err := Resolve(envMap(map[string]string{"POD_NAME": "arcade-gs-a-3", "MATCH_ID": "finals"}), Defaults{})
	if err != nil {
		t.Fatal(err)
	}
	if id.MatchID != "finals" {
		t.Fatalf("match = %q", id.MatchID)
	}
}

func TestResolveRejectsBadInput(t *testing.T) {
	if _, err := Resolve(envMap(map[string]string{"ROLE": "leader"}), Defaults{}); err == nil {
		t.Fatal("expected error for unknown role")
	}
	if _, err := Resolve(envMap(map[string]string{"HEARTBEAT_PORT": "70000"}), Defaults{}); err == nil {
		t.Fatal("expected error for bad port")
	}
}

func TestSplitOrdinal(t *testing.T) {
	cases := []struct {
		in   string
		set  string
		ord  int
		okay bool
	}{
		{"arcade-gs-a-0", "arcade-gs-a", 0, true},
		{"arcade-gs-b-123", "arcade-gs-b", 123, true},
		{"gameserver", "", 0, false},
		{"arcade-gs-a-", "", 0, false},
		{"arcade-gs-a-x1", "", 0, false},
	}
	for _, c := range cases {
		set, ord, ok := splitOrdinal(c.in)
		if ok != c.okay || (ok && (set != c.set || ord != c.ord)) {
			t.Errorf("splitOrdinal(%q) = %q,%d,%v", c.in, set, ord, ok)
		}
	}
}

// peerServer runs a fake partner pod serving /role.
func peerServer(t *testing.T, info RoleInfo) Identity {
	t.Helper()
	st := NewStatus(Identity{PodName: info.Pod, MatchID: info.MatchID}, info.Role)
	st.SetEpoch(info.Epoch)
	mux := http.NewServeMux()
	st.Register(mux)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	host, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	p, _ := strconv.Atoi(port)
	return Identity{PodName: "arcade-gs-a-0", MatchID: "match-000", InitialRole: RolePrimary, PeerHost: host, PeerHTTPPort: p}
}

func fastCfg() ArbitrateConfig {
	return ArbitrateConfig{Client: &http.Client{Timeout: 200 * time.Millisecond}, Attempts: 2, Backoff: 10 * time.Millisecond}
}

func TestArbitrateYieldsToPromotedPeer(t *testing.T) {
	id := peerServer(t, RoleInfo{Pod: "arcade-gs-b-0", MatchID: "match-000", Role: RolePrimary, Epoch: 4})
	d := Arbitrate(context.Background(), id, fastCfg())
	if d.Role != RoleBackup || d.Peer == nil || d.Peer.Epoch != 4 {
		t.Fatalf("decision = %+v", d)
	}
}

func TestArbitrateKeepsPrimaryWhenPeerIsBackup(t *testing.T) {
	id := peerServer(t, RoleInfo{Pod: "arcade-gs-b-0", MatchID: "match-000", Role: RoleBackup})
	if d := Arbitrate(context.Background(), id, fastCfg()); d.Role != RolePrimary {
		t.Fatalf("decision = %+v", d)
	}
}

func TestArbitrateIgnoresPeerOfOtherMatch(t *testing.T) {
	id := peerServer(t, RoleInfo{Pod: "x", MatchID: "match-999", Role: RolePrimary})
	if d := Arbitrate(context.Background(), id, fastCfg()); d.Role != RolePrimary {
		t.Fatalf("decision = %+v", d)
	}
}

func TestArbitratePeerUnreachable(t *testing.T) {
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close() // nothing listens here now
	id := Identity{MatchID: "match-000", InitialRole: RolePrimary, PeerHost: "127.0.0.1", PeerHTTPPort: port}
	if d := Arbitrate(context.Background(), id, fastCfg()); d.Role != RolePrimary || d.Peer != nil {
		t.Fatalf("decision = %+v", d)
	}
}

func TestArbitrateBackupNeverSelfPromotes(t *testing.T) {
	id := Identity{MatchID: "match-000", InitialRole: RoleBackup}
	if d := Arbitrate(context.Background(), id, fastCfg()); d.Role != RoleBackup {
		t.Fatalf("decision = %+v", d)
	}
}

func TestStatusHandlersAndTransitions(t *testing.T) {
	st := NewStatus(Identity{PodName: "arcade-gs-b-1", MatchID: "match-001"}, RoleBackup)
	mux := http.NewServeMux()
	st.Register(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before ready = %d", rec.Code)
	}
	st.MarkReady()
	st.Promote(9)

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after ready = %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/role", nil))
	var info RoleInfo
	if err := json.NewDecoder(rec.Body).Decode(&info); err != nil {
		t.Fatal(err)
	}
	if info.Role != RolePrimary || info.Epoch != 9 || !info.Ready || info.MatchID != "match-001" {
		t.Fatalf("role info = %+v", info)
	}
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/role", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /role = %d", rec.Code)
	}
	st.Demote()
	if st.IsPrimary() || st.Info().Epoch != 0 {
		t.Fatalf("after demote: %+v", st.Info())
	}
}

// fakeAPIServer records pod PATCH requests like the Kubernetes API server.
type fakeAPIServer struct {
	mu      sync.Mutex
	patches []map[string]any
	auth    []string
	fail    int // fail this many requests first
}

func (f *fakeAPIServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.Method != http.MethodPatch || r.URL.Path != "/api/v1/namespaces/arcade/pods/arcade-gs-b-1" ||
		r.Header.Get("Content-Type") != "application/merge-patch+json" {
		http.Error(w, "bad request "+r.Method+" "+r.URL.Path, http.StatusBadRequest)
		return
	}
	if f.fail > 0 {
		f.fail--
		http.Error(w, "etcd busy", http.StatusInternalServerError)
		return
	}
	body, _ := io.ReadAll(r.Body)
	var m map[string]any
	_ = json.Unmarshal(body, &m)
	f.patches = append(f.patches, m)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	_, _ = w.Write([]byte(`{}`))
}

func (f *fakeAPIServer) last() (map[string]any, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.patches) == 0 {
		return nil, 0
	}
	return f.patches[len(f.patches)-1], len(f.patches)
}

func labelOf(patch map[string]any, key string) string {
	md, _ := patch["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	s, _ := labels[key].(string)
	return s
}

func TestLabelerFollowsRoleChanges(t *testing.T) {
	api := &fakeAPIServer{fail: 1}
	srv := httptest.NewServer(api)
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	l := &Labeler{BaseURL: srv.URL, TokenFile: tokenFile, Namespace: "arcade", Pod: "arcade-gs-b-1", Client: srv.Client()}
	st := NewStatus(Identity{PodName: "arcade-gs-b-1", MatchID: "match-001"}, RoleBackup)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx, st, t.Logf)

	waitFor(t, func() bool { p, _ := api.last(); return labelOf(p, LabelRole) == "backup" })
	st.Promote(3)
	waitFor(t, func() bool { p, _ := api.last(); return labelOf(p, LabelRole) == "primary" })

	p, _ := api.last()
	if labelOf(p, LabelMatch) != "match-001" {
		t.Fatalf("match label missing: %v", p)
	}
	md := p["metadata"].(map[string]any)
	if ann := md["annotations"].(map[string]any); ann[AnnotationEpoch] != "3" {
		t.Fatalf("epoch annotation = %v", ann)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if api.auth[0] != "Bearer s3cret" {
		t.Fatalf("auth header = %q", api.auth[0])
	}
}

func TestInClusterLabelerOutsideCluster(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	if _, err := InClusterLabeler("arcade", "pod"); err != ErrNotInCluster {
		t.Fatalf("err = %v", err)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}
