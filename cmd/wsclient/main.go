// Command wsclient simulates players for the "client connection continuity"
// test: N WebSocket clients connect through the proxy, send "inc" at a steady
// rate, and watch for disconnects while the primary is replaced.
//
//	wsclient -url ws://127.0.0.1:8080/ws -clients 20 -duration 20s
//
// Replies are expected in the form "<server>|<match>|<n>" (see cmd/fakegame).
// The exit status is 1 if any client lost its connection, which is the
// pass/fail criterion: zero disconnects during retargeting.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type stats struct {
	mu        sync.Mutex
	sent      int
	received  int
	lastRecv  time.Time
	maxGap    time.Duration
	gapAt     time.Time
	lastWho   string
	lastN     int
	switches  []string // "A->B at +12.3s"
	regress   int      // counter went backwards or repeated
	skipped   int      // counter jumped forward by more than 1 (lost replies)
	disc      error
	discAt    time.Time
	connected bool
}

func main() {
	rawURL := flag.String("url", "ws://127.0.0.1:8080/ws", "proxy WebSocket URL (without query)")
	clients := flag.Int("clients", 10, "number of simulated players")
	duration := flag.Duration("duration", 20*time.Second, "how long to run")
	interval := flag.Duration("interval", 50*time.Millisecond, "time between messages per client")
	prefix := flag.String("match-prefix", "room", "match ids are <prefix><index>")
	perMatch := flag.Int("players-per-match", 1, "clients that share a match (their counters interleave, so sequence checks are skipped when >1)")
	flag.Parse()

	u, err := url.Parse(*rawURL)
	if err != nil {
		fmt.Fprintln(os.Stderr, "bad -url:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	start := time.Now()
	all := make([]*stats, *clients)
	var wg sync.WaitGroup
	for i := 0; i < *clients; i++ {
		all[i] = &stats{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := url.Values{}
			q.Set("match", fmt.Sprintf("%s%d", *prefix, i / *perMatch))
			q.Set("player", fmt.Sprintf("p%d", i))
			uu := *u
			uu.RawQuery = q.Encode()
			runClient(ctx, uu.String(), *interval, *perMatch == 1, start, all[i])
		}(i)
	}
	wg.Wait()

	// ---- report ----
	var sent, recv, disconnects, regress, skipped int
	var worstGap time.Duration
	var worstAt time.Time
	switchCount := map[string]int{}
	for _, s := range all {
		sent += s.sent
		recv += s.received
		regress += s.regress
		skipped += s.skipped
		if s.disc != nil {
			disconnects++
		}
		if s.maxGap > worstGap {
			worstGap, worstAt = s.maxGap, s.gapAt
		}
		for _, sw := range s.switches {
			name, _, _ := strings.Cut(sw, " at ")
			switchCount[name]++
		}
	}
	fmt.Printf("clients:            %d for %v\n", *clients, time.Since(start).Round(time.Millisecond))
	fmt.Printf("messages:           %d sent, %d answered, %d unanswered\n", sent, recv, sent-recv)
	fmt.Printf("disconnects:        %d\n", disconnects)
	fmt.Printf("counter anomalies:  %d repeated/backwards, %d gaps (lost replies)\n", regress, skipped)
	fmt.Printf("longest reply gap:  %v (at +%v)\n", worstGap.Round(time.Millisecond), worstAt.Sub(start).Round(time.Millisecond))
	if len(switchCount) > 0 {
		keys := make([]string, 0, len(switchCount))
		for k := range switchCount {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fmt.Printf("primary switches:   %s seen by %d clients\n", k, switchCount[k])
		}
	}
	for i, s := range all {
		if s.disc != nil {
			fmt.Printf("DISCONNECTED client %d at +%v: %v\n", i, s.discAt.Sub(start).Round(time.Millisecond), s.disc)
		}
	}
	if disconnects > 0 {
		fmt.Println("RESULT: FAIL")
		os.Exit(1)
	}
	fmt.Println("RESULT: PASS (no client was disconnected)")
}

func runClient(ctx context.Context, target string, interval time.Duration, checkSeq bool, start time.Time, st *stats) {
	dctx, dcancel := context.WithTimeout(ctx, 5*time.Second)
	c, _, err := websocket.Dial(dctx, target, nil)
	dcancel()
	if err != nil {
		st.mu.Lock()
		st.disc, st.discAt = fmt.Errorf("initial dial: %w", err), time.Now()
		st.mu.Unlock()
		return
	}
	defer func() { _ = c.CloseNow() }()
	st.mu.Lock()
	st.connected, st.lastRecv = true, time.Now()
	st.mu.Unlock()

	readDone := make(chan struct{})
	defer func() { _ = c.CloseNow(); <-readDone }() // report only after the reader has finished
	go func() {
		defer close(readDone)
		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				if ctx.Err() == nil { // not our own shutdown
					st.mu.Lock()
					st.disc, st.discAt = err, time.Now()
					st.mu.Unlock()
				}
				return
			}
			now := time.Now()
			parts := strings.Split(string(data), "|")
			st.mu.Lock()
			st.received++
			if gap := now.Sub(st.lastRecv); gap > st.maxGap {
				st.maxGap, st.gapAt = gap, now
			}
			st.lastRecv = now
			if len(parts) == 3 {
				who := parts[0]
				var n int
				_, _ = fmt.Sscanf(parts[2], "%d", &n)
				if st.lastWho != "" && who != st.lastWho {
					st.switches = append(st.switches, fmt.Sprintf("%s->%s at +%v", st.lastWho, who, now.Sub(start).Round(time.Millisecond)))
				}
				if checkSeq && st.lastN != 0 {
					switch {
					case n <= st.lastN:
						st.regress++
					case n > st.lastN+1:
						st.skipped += n - st.lastN - 1
					}
				}
				st.lastWho, st.lastN = who, n
			}
			st.mu.Unlock()
		}
	}()

	tick := time.NewTicker(interval)
	defer tick.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-readDone:
			return
		case <-tick.C:
			wctx, wcancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := c.Write(wctx, websocket.MessageText, []byte("inc"))
			wcancel()
			if err != nil {
				st.mu.Lock()
				if st.disc == nil {
					st.disc, st.discAt = fmt.Errorf("write: %w", err), time.Now()
				}
				st.mu.Unlock()
				return
			}
			st.mu.Lock()
			st.sent++
			st.mu.Unlock()
		}
	}
	// Our own shutdown: stop sending, give the last replies a moment, close.
	select {
	case <-readDone:
	case <-time.After(300 * time.Millisecond):
	}
	_ = c.Close(websocket.StatusNormalClosure, "test finished")
}
