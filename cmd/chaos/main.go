// Command chaos injects faults into the ARCADE fleet and times the failover.
//
//	chaos -target gameserver -mode sigkill          local process (original behaviour)
//	chaos -mode k8s-crash -pair 0 -runs 20          force-delete the CURRENT primary pod
//	chaos -mode partition -block udp,redis -duration 10s
//	chaos -mode partition -block udp                heartbeat dead, Redis alive (expect NO takeover)
//	chaos -mode loss  -percent 10 -duration 10s     packet loss on the primary's egress
//	chaos -mode delay -delay 50ms -jitter 20ms -duration 10s
//
// The Kubernetes modes shell out to kubectl, so they use your current kube
// context and need no extra Go dependencies.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"time"
)

type config struct {
	target    string
	mode      string
	namespace string
	pair      int
	runs      int
	settle    time.Duration
	window    time.Duration
	duration  time.Duration
	block     string
	percent   int
	delay     time.Duration
	jitter    time.Duration
	dev       string
	image     string
	hbPort    int
	redisPort int
	reset     bool
	healWait  time.Duration
	warmup    time.Duration
}

func main() {
	var c config
	flag.StringVar(&c.target, "target", "", "sigkill mode only: process name to pkill -9 on this machine")
	flag.StringVar(&c.mode, "mode", "sigkill", "sigkill | k8s-crash | partition | loss | delay")
	flag.StringVar(&c.namespace, "namespace", "arcade", "Kubernetes namespace")
	flag.IntVar(&c.pair, "pair", 0, "pair index i (arcade-gs-a-i / arcade-gs-b-i)")
	flag.IntVar(&c.runs, "runs", 1, "how many injections to run")
	flag.DurationVar(&c.settle, "settle", 3*time.Second, "pause before each injection so the game is mid-play")
	flag.DurationVar(&c.window, "window", 30*time.Second, "k8s-crash: how long to wait for a takeover")
	flag.DurationVar(&c.duration, "duration", 10*time.Second, "network faults: how long the fault lasts")
	flag.StringVar(&c.block, "block", "udp,redis", "partition: what to cut, any of udp,redis")
	flag.IntVar(&c.percent, "percent", 10, "loss mode: percent of packets dropped")
	flag.DurationVar(&c.delay, "delay", 50*time.Millisecond, "delay mode: added latency")
	flag.DurationVar(&c.jitter, "jitter", 20*time.Millisecond, "delay mode: latency jitter")
	flag.StringVar(&c.dev, "dev", "eth0", "network interface inside the pod")
	flag.StringVar(&c.image, "image", "nicolaka/netshoot", "debug image carrying iptables and tc")
	flag.IntVar(&c.hbPort, "hb-port", 9000, "UDP heartbeat port")
	flag.IntVar(&c.redisPort, "redis-port", 6379, "Redis TCP port")
	flag.BoolVar(&c.reset, "reset", false, "restart BOTH pods of the pair before every run, so each run starts clean")
	flag.DurationVar(&c.healWait, "heal-wait", 45*time.Second, "how long to wait after a fault for the pair to heal before judging it")
	flag.DurationVar(&c.warmup, "warmup", 30*time.Second, "with -reset: wait this long after the restart so the game is being played")
	flag.Parse()

	switch c.mode {
	case "sigkill":
		if c.target == "" {
			fmt.Println("Usage: chaos -target <process_name_or_pid> -mode <sigkill|k8s-crash|partition|loss|delay>")
			os.Exit(1)
		}
		localSigkill(c.target)
	case "k8s-crash", "partition", "loss", "delay":
		if err := runK8s(c); err != nil {
			log.Fatalf("[CHAOS] %v", err)
		}
	default:
		log.Fatalf("[CHAOS] unsupported mode %q", c.mode)
	}
}

// localSigkill kills a process on THIS machine: by PID if the target is a
// number, otherwise by name pattern with pkill (Linux/macOS/Git Bash with pkill).
func localSigkill(target string) {
	log.Printf("[CHAOS] Executing fault injection: target=%s mode=sigkill", target)

	// A numeric target is a PID. os.Process.Kill is SIGKILL on Linux/macOS and
	// TerminateProcess on Windows, so this compiles and works on both
	// (syscall.Kill does not exist on Windows).
	if pid, err := strconv.Atoi(target); err == nil {
		proc, err := os.FindProcess(pid)
		if err == nil {
			err = proc.Kill()
		}
		if err != nil {
			log.Fatalf("[CHAOS] kill -9 on PID %d failed: %v", pid, err)
		}
		log.Printf("[CHAOS] Successfully sent SIGKILL to PID %d", pid)
		return
	}

	out, err := exec.Command("pkill", "-9", "-f", target).CombinedOutput()
	if err != nil {
		log.Printf("[CHAOS] pkill completed with: %v (out: %s)", err, string(out))
		return
	}
	log.Printf("[CHAOS] Successfully terminated target %s", target)
}
