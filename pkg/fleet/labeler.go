package fleet

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Pod labels and annotations maintained by the Labeler.
const (
	LabelRole       = "arcade.io/role"
	LabelMatch      = "arcade.io/match"
	AnnotationEpoch = "arcade.io/epoch"
)

const serviceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// ErrNotInCluster is returned by InClusterLabeler outside Kubernetes.
var ErrNotInCluster = errors.New("fleet: not running inside Kubernetes")

// Labeler mirrors the pod's live role onto its own Kubernetes labels, so
// `kubectl get pods -L arcade.io/role` always shows who the primary is and
// label selectors (e.g. a Service or the proxy) can find it.
//
// It speaks to the API server with plain net/http (a JSON merge-patch on the
// pod) instead of pulling in client-go. The pod's ServiceAccount needs
// "patch" on "pods" in its namespace; see k8s/base/rbac.yaml.
type Labeler struct {
	BaseURL   string // https://<host>:<port>
	TokenFile string
	Namespace string
	Pod       string
	Client    *http.Client
}

// InClusterLabeler configures a Labeler from the standard in-cluster
// ServiceAccount mount and KUBERNETES_SERVICE_* environment.
func InClusterLabeler(namespace, pod string) (*Labeler, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" || pod == "" {
		return nil, ErrNotInCluster
	}
	ca, err := os.ReadFile(serviceAccountDir + "/ca.crt")
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotInCluster, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, errors.New("fleet: invalid service account CA bundle")
	}
	return &Labeler{
		BaseURL:   "https://" + net.JoinHostPort(host, port),
		TokenFile: serviceAccountDir + "/token",
		Namespace: namespace,
		Pod:       pod,
		Client: &http.Client{
			Timeout:   5 * time.Second,
			Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
		},
	}, nil
}

// Apply patches the pod's labels/annotations to match info.
func (l *Labeler) Apply(ctx context.Context, info RoleInfo) error {
	patch := map[string]any{
		"metadata": map[string]any{
			"labels": map[string]string{
				LabelRole:  string(info.Role),
				LabelMatch: info.MatchID,
			},
			"annotations": map[string]string{
				AnnotationEpoch: strconv.FormatInt(info.Epoch, 10),
			},
		},
	}
	body, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s/api/v1/namespaces/%s/pods/%s", strings.TrimRight(l.BaseURL, "/"), l.Namespace, l.Pod)
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/merge-patch+json")
	req.Header.Set("Accept", "application/json")
	if l.TokenFile != "" {
		// Re-read on every call: projected ServiceAccount tokens rotate.
		tok, err := os.ReadFile(l.TokenFile)
		if err != nil {
			return fmt.Errorf("fleet: read token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(string(tok)))
	}
	client := l.Client
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("fleet: patch pod labels: %s: %s", resp.Status, strings.TrimSpace(string(msg)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Run keeps the pod labels in sync with status until ctx is cancelled.
// Changes are coalesced: the worker always patches the latest state, so a
// burst of transitions costs at most one in-flight request plus one more.
func (l *Labeler) Run(ctx context.Context, status *Status, logf func(string, ...any)) {
	if logf == nil {
		logf = func(string, ...any) {}
	}
	kick := make(chan struct{}, 1)
	status.OnChange(func(RoleInfo) {
		select {
		case kick <- struct{}{}:
		default:
		}
	})
	for {
		select {
		case <-ctx.Done():
			return
		case <-kick:
		}
		backoff := 200 * time.Millisecond
		for attempt := 1; ; attempt++ {
			info := status.Info()
			reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err := l.Apply(reqCtx, info)
			cancel()
			if err == nil {
				logf("[FLEET] pod labels set: %s=%s %s=%s epoch=%d", LabelRole, info.Role, LabelMatch, info.MatchID, info.Epoch)
				break
			}
			logf("[FLEET] label patch attempt %d failed: %v", attempt, err)
			if attempt >= 5 {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff *= 2
		}
	}
}
