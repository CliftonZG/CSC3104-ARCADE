package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"
)

func runK8s(c config) error {
	k := kube{ns: c.namespace}
	fmt.Println("run,mode,victim,promoted,recovery_ms,pair_after")

	var times []time.Duration
	missed, splits := 0, 0
	for run := 1; run <= c.runs; run++ {
		victim, err := k.prepare(c)
		if err != nil {
			return err
		}

		ctx, cancel := context.WithCancel(context.Background())
		w, err := k.watchPromotion(ctx, c.pair, victim)
		if err != nil {
			cancel()
			return err
		}
		select {
		case <-w.started:
		case <-time.After(15 * time.Second):
			cancel()
			return errors.New("pod watch did not start")
		}

		start, window, err := inject(k, c, victim)
		if err != nil {
			cancel()
			return err
		}

		promoted, recovery := "-", "none"
		select {
		case p, ok := <-w.result:
			if ok {
				d := p.At.Sub(start)
				promoted = p.Pod
				if d < 0 {
					log.Printf("[CHAOS] promotion for %s was observed before fault activation; excluding invalid recovery time %s", p.Pod, d)
					recovery = "invalid"
				} else {
					times = append(times, d)
					recovery = fmt.Sprint(d.Milliseconds())
				}
			}
		case <-time.After(window):
		}
		if promoted == "-" {
			missed++
		}
		cancel()

		if c.mode != "k8s-crash" { // wait for the fault to end before judging the pair
			time.Sleep(time.Until(start.Add(c.duration + 2*time.Second)))
		}

		// Did the pair heal by itself?
		st := k.awaitHealthy(c.pair, c.healWait)
		after := st.kind()
		if after == "split-brain" {
			splits++
			log.Printf("[CHAOS] SPLIT BRAIN on pair %d: %s", c.pair, st)
		} else if after != "healthy" {
			log.Printf("[CHAOS] pair %d is %s after the run: %s", c.pair, after, st)
		}
		fmt.Printf("%d,%s,%s,%s,%s,%s\n", run, c.mode, victim, promoted, recovery, after)
		log.Printf("[CHAOS] run %d/%d done", run, c.runs)
	}
	summarize(times, missed, splits)
	return nil
}

// prepare makes sure the pair is healthy (resetting a split brain if allowed)
// and returns the current primary.
func (k kube) prepare(c config) (string, error) {
	if c.reset {
		if err := k.resetPair(c.pair, c.warmup); err != nil {
			return "", err
		}
	} else if st, err := k.pairState(c.pair); err == nil && st.kind() == "split-brain" {
		return "", fmt.Errorf("pair %d is in split-brain (%s): rerun with -reset", c.pair, st)
	}
	st := k.awaitHealthy(c.pair, 3*time.Minute)
	if st.kind() != "healthy" {
		return "", fmt.Errorf("pair %d did not become healthy (%s): %s", c.pair, st.kind(), st)
	}
	time.Sleep(c.settle) // let a few moves happen: fault lands MID-game
	return st.primary(), nil
}

// inject applies the chosen fault and returns when it started and how long to wait for a takeover.
func inject(k kube, c config, victim string) (time.Time, time.Duration, error) {
	window := c.duration + 15*time.Second
	switch c.mode {
	case "k8s-crash":
		start, err := k.crash(victim)
		return start, c.window, err
	case "partition":
		s, err := partitionScript(c.block, c.duration, c.hbPort, c.redisPort)
		if err != nil {
			return time.Time{}, 0, err
		}
		start, err := k.netFault(victim, c.image, s)
		return start, window, err
	case "loss":
		start, err := k.netFault(victim, c.image,
			netemScript(c.dev, fmt.Sprintf("loss %d%%", c.percent), c.duration))
		return start, window, err
	default: // delay
		spec := fmt.Sprintf("delay %dms %dms", c.delay.Milliseconds(), c.jitter.Milliseconds())
		start, err := k.netFault(victim, c.image, netemScript(c.dev, spec, c.duration))
		return start, window, err
	}
}

func summarize(times []time.Duration, missed, splits int) {
	log.Printf("[CHAOS] takeovers: %d, no takeover: %d, split-brain after run: %d", len(times), missed, splits)
	if len(times) == 0 {
		return
	}
	sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
	pct := func(p float64) time.Duration { return times[int(p*float64(len(times)-1))] }
	log.Printf("[CHAOS] recovery min=%s median=%s p95=%s max=%s",
		times[0], pct(0.5), pct(0.95), times[len(times)-1])
}
