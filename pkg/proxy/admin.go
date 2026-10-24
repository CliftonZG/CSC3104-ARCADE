package proxy

import (
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// maxAdminBody bounds the size of a route update.
const maxAdminBody = 4 << 10

// AdminHandler serves the control-plane-facing API. Bind it to an internal
// address only; it can redirect any match to any host:port.
//
//	POST /admin/route    {"match_id","epoch","primary_addr"}  -> {"result": applied|stale|duplicate|conflict}
//	GET  /admin/routes   current route table
//	GET  /stats          counters
//	GET  /healthz        liveness
//
// If Config.AdminToken is set, the /admin/ endpoints require
// "Authorization: Bearer <token>".
func (p *Proxy) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /admin/route", p.auth(p.handleRoute))
	mux.HandleFunc("GET /admin/routes", p.auth(p.handleRoutes))
	mux.HandleFunc("GET /stats", p.handleStats)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok\n")
	})
	return mux
}

func (p *Proxy) auth(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if tok := p.cfg.AdminToken; tok != "" {
			got := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") ||
				subtle.ConstantTimeCompare([]byte(got), []byte(tok)) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="arcade-proxy-admin"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ApplyUpdate applies a takeover notification and records the outcome in the
// stats. It is what the admin endpoint, and any other transport (such as a
// Redis subscriber), should call.
func (p *Proxy) ApplyUpdate(u Update) (ApplyResult, error) {
	res, err := p.table.Apply(u)
	if err != nil {
		return "", err
	}
	if res == Applied {
		p.stats.RoutesApplied.Add(1)
		p.log.Info("route updated", "match", u.MatchID, "epoch", u.Epoch, "primary", u.PrimaryAddr)
	} else {
		p.stats.RoutesIgnored.Add(1)
		p.log.Info("route update ignored", "result", string(res), "match", u.MatchID, "epoch", u.Epoch, "primary", u.PrimaryAddr)
	}
	return res, nil
}

func (p *Proxy) handleRoute(w http.ResponseWriter, r *http.Request) {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxAdminBody))
	dec.DisallowUnknownFields()
	var u Update
	if err := dec.Decode(&u); err != nil {
		http.Error(w, "bad request body: "+err.Error(), http.StatusBadRequest)
		return
	}
	if dec.More() {
		http.Error(w, "bad request body: trailing data", http.StatusBadRequest)
		return
	}
	res, err := p.ApplyUpdate(u)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	status := http.StatusOK
	if res == Conflict {
		status = http.StatusConflict
	}
	writeJSON(w, status, map[string]string{"result": string(res)})
}

func (p *Proxy) handleRoutes(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, p.table.Snapshot())
}

func (p *Proxy) handleStats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, p.stats.Snapshot())
}
