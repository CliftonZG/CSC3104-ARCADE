package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const gsSelector = "app.kubernetes.io/name=arcade-gameserver"

type kube struct{ ns string }

// pod holds only the fields we read from `kubectl get pod -o json`.
type pod struct {
	Metadata struct {
		Name              string            `json:"name"`
		Labels            map[string]string `json:"labels"`
		DeletionTimestamp *time.Time        `json:"deletionTimestamp"`
	} `json:"metadata"`
}

func (p pod) role() string { return p.Metadata.Labels["arcade.io/role"] }
func (p pod) dying() bool  { return p.Metadata.DeletionTimestamp != nil }
func (p pod) name() string { return p.Metadata.Name }

func (k kube) kubectl(args ...string) ([]byte, error) {
	cmd := exec.Command("kubectl", append([]string{"-n", k.ns}, args...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return out, nil
}

func pairSelector(idx int) string {
	return fmt.Sprintf("%s,apps.kubernetes.io/pod-index=%d", gsSelector, idx)
}

// pairPods returns arcade-gs-a-<idx> and arcade-gs-b-<idx>.
func (k kube) pairPods(idx int) ([]pod, error) {
	out, err := k.kubectl("get", "pods", "-l", pairSelector(idx), "-o", "json")
	if err != nil {
		return nil, err
	}
	var list struct {
		Items []pod `json:"items"`
	}
	if err := json.Unmarshal(out, &list); err != nil {
		return nil, err
	}
	return list.Items, nil
}

func settled(pods []pod) bool {
	if len(pods) != 2 {
		return false
	}
	roles := map[string]int{}
	for _, p := range pods {
		if p.dying() {
			return false
		}
		roles[p.role()]++
	}
	return roles["primary"] == 1 && roles["backup"] == 1
}

type promotion struct {
	Pod string
	At  time.Time
}

type watcher struct {
	started <-chan struct{}  // closed once the watch is live
	result  <-chan promotion // the takeover, or closed with no value if none arrived
}

// watchPromotion streams pod changes for one pair and reports the first pod
// OTHER than the victim that turns primary. The victim is excluded by name
// because a restarted pod briefly carries the template label role=primary.
func (k kube) watchPromotion(ctx context.Context, idx int, victim string) (*watcher, error) {
	cmd := exec.CommandContext(ctx, "kubectl", "-n", k.ns, "get", "pods",
		"-l", pairSelector(idx), "-w", "--output-watch-events", "-o", "json")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	started := make(chan struct{})
	result := make(chan promotion, 1)
	var once sync.Once
	go func() {
		defer close(result)
		dec := json.NewDecoder(stdout)
		for {
			var ev struct {
				Type   string `json:"type"`
				Object pod    `json:"object"`
			}
			if err := dec.Decode(&ev); err != nil {
				break
			}
			at := time.Now()
			once.Do(func() { close(started) })
			if ev.Object.name() != victim && ev.Object.role() == "primary" && !ev.Object.dying() {
				result <- promotion{Pod: ev.Object.name(), At: at}
				break
			}
		}
		_, _ = io.Copy(io.Discard, stdout) // drain until the caller cancels ctx
		_ = cmd.Wait()
	}()
	return &watcher{started: started, result: result}, nil
}
