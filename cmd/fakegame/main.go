// Command fakegame is a stand-in game pod for exercising the proxy: it counts.
//
// Each WebSocket message "inc" increments the match's counter and is answered
// with "<name>|<match>|<counter>". The counters live in a JSON file given by
// -state, which several fakegame processes can share. That file plays the role
// Redis plays in the real system, so after a failover the new primary carries
// on from where the old one stopped.
//
// Endpoints:
//
//	/ws?match=<id>   the game socket (what the proxy dials)
//	POST /demote     step down: close every socket with 1012, then refuse new ones with 503
//	GET /role        {"role":"primary|backup","epoch":N,...}, what the proxy's -discover-pods asks
//	POST /promote?epoch=N   become primary at epoch N (what the failure detector does on a takeover)
//	/healthz
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/coder/websocket"
)

type store struct {
	mu   sync.Mutex
	path string
}

// inc atomically (within this process) bumps the counter for a match. With a
// state file it is read and rewritten on every call, so a second process sees
// the first one's progress. Good enough for a demo with one primary at a time.
func (s *store) inc(match string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	counters := map[string]int{}
	if s.path != "" {
		b, err := os.ReadFile(s.path)
		if err == nil {
			if err := json.Unmarshal(b, &counters); err != nil {
				return 0, fmt.Errorf("state file is corrupt: %w", err)
			}
		} else if !os.IsNotExist(err) {
			return 0, err
		}
	}
	counters[match]++
	n := counters[match]
	if s.path != "" {
		b, _ := json.Marshal(counters)
		tmp := s.path + ".tmp"
		if err := os.WriteFile(tmp, b, 0o644); err != nil {
			return 0, err
		}
		if err := os.Rename(tmp, s.path); err != nil {
			return 0, err
		}
	}
	return n, nil
}

func main() {
	name := flag.String("name", "A", "name shown in replies")
	listen := flag.String("listen", "127.0.0.1:9001", "listen address")
	role0 := flag.String("role", "primary", "initial role reported on /role: primary or backup")
	epoch0 := flag.Int64("epoch", 1, "initial epoch reported on /role")
	state := flag.String("state", "", "shared state file (stand-in for Redis); empty keeps counters in memory only")
	flag.Parse()

	st := &store{path: *state}
	var (
		roleMu sync.Mutex
		role   = *role0
		epoch  = *epoch0
	)
	var (
		mu      sync.Mutex
		conns   = map[*websocket.Conn]struct{}{}
		demoted = *role0 == "backup" // a backup refuses handshakes with 503, like a real standby
		// proc makes "apply a message and send its reply" exclusive with
		// stepping down, so a message is never applied whose reply can no
		// longer be sent.
		proc sync.Mutex
	)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprintln(w, "ok") })
	mux.HandleFunc("GET /role", func(w http.ResponseWriter, _ *http.Request) {
		roleMu.Lock()
		r, e := role, epoch
		roleMu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"pod": *name, "role": r, "epoch": e, "ready": true})
	})
	mux.HandleFunc("POST /promote", func(w http.ResponseWriter, r *http.Request) {
		e, err := strconv.ParseInt(r.URL.Query().Get("epoch"), 10, 64)
		if err != nil || e < 0 {
			http.Error(w, "need ?epoch=N", http.StatusBadRequest)
			return
		}
		mu.Lock()
		demoted = false
		mu.Unlock()
		roleMu.Lock()
		role, epoch = "primary", e
		roleMu.Unlock()
		log.Printf("[%s] promoted to primary at epoch %d", *name, e)
		_, _ = fmt.Fprintln(w, "promoted")
	})
	mux.HandleFunc("POST /demote", func(w http.ResponseWriter, _ *http.Request) {
		proc.Lock() // wait for in-flight messages to finish, reply included
		mu.Lock()
		demoted = true
		proc.Unlock()
		cs := make([]*websocket.Conn, 0, len(conns))
		for c := range conns {
			cs = append(cs, c)
		}
		mu.Unlock()
		for _, c := range cs {
			go func(c *websocket.Conn) { _ = c.Close(websocket.StatusServiceRestart, "stepping down") }(c)
		}
		roleMu.Lock()
		role = "backup"
		roleMu.Unlock()
		log.Printf("[%s] demoted; closing %d sockets with 1012", *name, len(cs))
		_, _ = fmt.Fprintln(w, "demoted")
	})
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		match := r.URL.Query().Get("match")
		mu.Lock()
		isDemoted := demoted
		mu.Unlock()
		if isDemoted {
			http.Error(w, "not primary", http.StatusServiceUnavailable)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		mu.Lock()
		conns[c] = struct{}{}
		mu.Unlock()
		defer func() {
			mu.Lock()
			delete(conns, c)
			mu.Unlock()
			_ = c.CloseNow()
		}()
		log.Printf("[%s] match %q connected (epoch %s, forwarded for %s)", *name, match,
			r.Header.Get("X-Arcade-Epoch"), r.Header.Get("X-Forwarded-For"))

		for {
			_, data, err := c.Read(context.Background())
			if err != nil {
				log.Printf("[%s] match %q socket ended: %v", *name, match, err)
				return
			}
			proc.Lock()
			mu.Lock()
			stepDown := demoted
			mu.Unlock()
			if stepDown {
				// A primary that has started closing must not change state:
				// the new primary owns it from here on.
				proc.Unlock()
				continue
			}
			if strings.TrimSpace(string(data)) != "inc" {
				err = c.Write(context.Background(), websocket.MessageText, []byte(*name+"|"+match+"|?"))
				proc.Unlock()
				if err != nil {
					return
				}
				continue
			}
			n, err := st.inc(match)
			if err != nil {
				proc.Unlock()
				log.Printf("[%s] state error: %v", *name, err)
				_ = c.Close(websocket.StatusInternalError, "state error")
				return
			}
			err = c.Write(context.Background(), websocket.MessageText, []byte(fmt.Sprintf("%s|%s|%d", *name, match, n)))
			proc.Unlock()
			if err != nil {
				return
			}
		}
	})

	srv := &http.Server{Addr: *listen, Handler: mux}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
		<-sig
		_ = srv.Close()
	}()
	log.Printf("[%s] listening on %s (state file %q)", *name, *listen, *state)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
