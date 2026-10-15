package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	port := flag.Int("port", 8080, "client proxy ingress port")
	primaryAddr := flag.String("primary", "localhost:9001", "primary pod upstream address")
	backupAddr := flag.String("backup", "localhost:9002", "backup pod upstream address")
	flag.Parse()

	log.Printf("[PROXY] Initializing ingress proxy on port %d", *port)
	log.Printf("[PROXY] Active primary upstream: %s, Standby upstream: %s", *primaryAddr, *backupAddr)

	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("OK"))
	})

	server := &http.Server{
		Addr: fmt.Sprintf(":%d", *port),
	}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[PROXY] HTTP server error: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	log.Printf("[PROXY] Received signal %v, shutting down ingress gateway.", sig)
	_ = server.Close()
}
