package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

const gsHTTPPort = 8081

// roleInfo is what each gameserver answers on GET /role.
type roleInfo struct {
	Pod   string `json:"pod"`
	Role  string `json:"role"`
	Epoch int64  `json:"epoch"`
}

// pairState is what both pods of one pair say about themselves.
type pairState struct{ Pods []roleInfo }

// roleOf asks one pod "what role do you think you have?" through the API
// server proxy, so no port-forward is needed. This is the pod's own belief,
// which is more truthful than the pod label.
func (k kube) roleOf(podName string) (roleInfo, error) {
	out, err := k.kubectl("get", "--raw",
		fmt.Sprintf("/api/v1/namespaces/%s/pods/%s:%d/proxy/role", k.ns, podName, gsHTTPPort))
	if err != nil {
		return roleInfo{}, err
	}
	var r roleInfo
	if err := json.Unmarshal(out, &r); err != nil {
		return roleInfo{}, err
	}
	if r.Pod == "" {
		r.Pod = podName
	}
	return r, nil
}

func (k kube) pairState(idx int) (pairState, error) {
	pods, err := k.pairPods(idx)
	if err != nil {
		return pairState{}, err
	}
	var st pairState
	for _, p := range pods {
		if p.dying() {
			continue
		}
		r, err := k.roleOf(p.name())
		if err != nil {
			return pairState{}, err
		}
		st.Pods = append(st.Pods, r)
	}
	return st, nil
}

// kind is healthy | split-brain | no-primary | unsettled | unknown.
func (s pairState) kind() string {
	if len(s.Pods) != 2 {
		return "unknown"
	}
	primaries := 0
	for _, p := range s.Pods {
		if p.Role == "primary" {
			primaries++
		}
	}
	switch primaries {
	case 2:
		return "split-brain"
	case 0:
		return "no-primary"
	}
	for _, p := range s.Pods {
		if p.Role != "primary" && p.Role != "backup" {
			return "unsettled"
		}
	}
	return "healthy"
}

func (s pairState) String() string {
	parts := make([]string, 0, len(s.Pods))
	for _, p := range s.Pods {
		parts = append(parts, fmt.Sprintf("%s=%s(epoch %d)", p.Pod, p.Role, p.Epoch))
	}
	return strings.Join(parts, ", ")
}

// primary returns the single primary's pod name ("" if there is not exactly one).
func (s pairState) primary() string {
	name := ""
	for _, p := range s.Pods {
		if p.Role == "primary" {
			if name != "" {
				return ""
			}
			name = p.Pod
		}
	}
	return name
}

// stale picks the pod to restart in a split brain: the primary with the LOWER
// epoch, i.e. the one that was replaced. It refuses to guess on a tie.
func (s pairState) stale() (string, bool) {
	var prim []roleInfo
	for _, p := range s.Pods {
		if p.Role == "primary" {
			prim = append(prim, p)
		}
	}
	if len(prim) != 2 {
		return "", false
	}
	sort.Slice(prim, func(i, j int) bool { return prim[i].Epoch < prim[j].Epoch })
	if prim[0].Epoch == prim[1].Epoch {
		return "", false
	}
	return prim[0].Pod, true
}

// awaitHealthy polls until the pair is one primary + one backup, or time runs out.
func (k kube) awaitHealthy(idx int, wait time.Duration) pairState {
	deadline := time.Now().Add(wait)
	var last pairState
	for {
		if st, err := k.pairState(idx); err == nil {
			last = st
			if st.kind() == "healthy" {
				return st
			}
		}
		if time.Now().After(deadline) {
			return last
		}
		time.Sleep(2 * time.Second)
	}
}

// resetPair restarts BOTH pods of the pair and waits until they are healthy
// again. Restarting only the stale pod left a backup that never learned the
// current epoch (so its promotions were always fenced) and a game that had
// stopped being played. Two fresh pods start from the same clean state.
func (k kube) resetPair(idx int, warmup time.Duration) error {
	pods, err := k.pairPods(idx)
	if err != nil {
		return err
	}
	args := []string{"delete", "pod"}
	for _, p := range pods {
		args = append(args, p.name())
	}
	log.Printf("[CHAOS] resetting pair %d (restarting %d pods)", idx, len(pods))
	if _, err := k.kubectl(args...); err != nil {
		return err
	}
	after := k.awaitHealthy(idx, 3*time.Minute)
	if after.kind() != "healthy" {
		return fmt.Errorf("pair %d is %s after reset: %s", idx, after.kind(), after)
	}
	log.Printf("[CHAOS] pair %d is back (%s); warming up %s so the game is being played", idx, after, warmup)
	time.Sleep(warmup)
	return nil
}
