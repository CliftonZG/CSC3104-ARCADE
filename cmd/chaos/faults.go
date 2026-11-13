package main

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// crash force-deletes the pod with no grace period: no SIGTERM, no goodbye.
// The returned time is taken just before the API call, so recovery is
// slightly over-estimated (kubelet needs a moment to kill the process).
func (k kube) crash(podName string) (time.Time, error) {
	start := time.Now()
	_, err := k.kubectl("delete", "pod", podName, "--grace-period=0", "--force", "--wait=false")
	return start, err
}

// The pods run as non-root with a read-only filesystem, and the netadmin
// profile only ADDS capabilities. iptables/tc need real root, so the
// ephemeral debug container overrides the pod's runAsNonRoot.
const customProfile = `{"securityContext":{"runAsUser":0,"runAsGroup":0,"runAsNonRoot":false}}`

// netFault attaches a short-lived root container to the pod. The container
// applies the fault, sleeps, then removes it again via a shell trap, so the
// network heals itself even if this CLI is killed. It returns when the CLI
// observes that the fault is in place, using the local clock.
func (k kube) netFault(podName, image, script string) (time.Time, error) {
	container := fmt.Sprintf("chaos-%d", time.Now().Unix())
	f, err := os.CreateTemp("", "chaos-custom-*.json")
	if err != nil {
		return time.Time{}, err
	}
	defer func() { _ = os.Remove(f.Name()) }()
	if _, err := f.WriteString(customProfile); err != nil {
		_ = f.Close()
		return time.Time{}, err
	}
	if err := f.Close(); err != nil {
		return time.Time{}, err
	}

	if _, err := k.kubectl("debug", podName, "--image="+image, "--target=gameserver",
		"--container="+container, "--profile=netadmin", "--custom="+f.Name(),
		"--", "sh", "-c", script); err != nil {
		return time.Time{}, err
	}
	return k.waitInjected(podName, container, 90*time.Second)
}

func (k kube) waitInjected(podName, container string, timeout time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if out, err := k.kubectl("logs", podName, "-c", container); err == nil {
			if fields := strings.Fields(string(out)); len(fields) > 0 {
				return time.Now(), nil
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	return time.Time{}, fmt.Errorf("fault on %s was not applied within %s (image pull? RBAC? see kubectl describe pod)", podName, timeout)
}

// script wraps "apply" and "undo" so undo ALWAYS runs on exit.
func script(apply, undo []string, hold time.Duration) string {
	secs := int(hold.Seconds())
	if secs < 1 {
		secs = 1
	}
	return fmt.Sprintf("set -e; %s; trap '%s' EXIT TERM INT; echo ready; sleep %d",
		strings.Join(apply, "; "), strings.Join(undo, "; "), secs)
}

func partitionScript(block string, hold time.Duration, hbPort, redisPort int) (string, error) {
	var apply, undo []string
	add := func(rule string) {
		apply = append(apply, "iptables -w -A "+rule)
		undo = append(undo, "iptables -w -D "+rule)
	}
	for _, what := range strings.Split(block, ",") {
		switch strings.TrimSpace(what) {
		case "udp": // heartbeat only; DNS (udp/53) keeps working
			add(fmt.Sprintf("INPUT -p udp --dport %d -j DROP", hbPort))
			add(fmt.Sprintf("OUTPUT -p udp --dport %d -j DROP", hbPort))
		case "redis":
			add(fmt.Sprintf("OUTPUT -p tcp --dport %d -j DROP", redisPort))
			add(fmt.Sprintf("INPUT -p tcp --sport %d -j DROP", redisPort))
		default:
			return "", fmt.Errorf("unknown -block value %q (use udp, redis)", what)
		}
	}
	return script(apply, undo, hold), nil
}

func netemScript(dev, spec string, hold time.Duration) string {
	apply := []string{fmt.Sprintf("tc qdisc replace dev %s root netem %s", dev, spec)}
	undo := []string{fmt.Sprintf("tc qdisc del dev %s root", dev)}
	return script(apply, undo, hold)
}
