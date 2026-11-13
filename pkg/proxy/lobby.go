package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"time"
)

// RoomSummary mirrors the gameserver's /api/rooms item for proxy aggregation.
type RoomSummary struct {
	MatchID           string   `json:"match_id"`
	Status            string   `json:"status"`
	Sequence          int64    `json:"sequence"`
	Winner            string   `json:"winner,omitempty"`
	Players           []string `json:"players"`
	OccupiedSeats     []string `json:"occupied_seats"`
	OpenSeats         []string `json:"open_seats"`
	ActiveConnections int      `json:"active_connections"`
}

type roomsEnvelope struct {
	Rooms []RoomSummary `json:"rooms"`
}

// HandleRooms serves GET /api/rooms (aggregating across known primary pods)
// and POST /api/rooms (forwarding room creation to the target primary pod).
func (p *Proxy) HandleRooms(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		rooms, err := p.fetchAllRooms(r.Context())
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"rooms": rooms})
	case http.MethodPost:
		body, err := io.ReadAll(io.LimitReader(r.Body, 4096))
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "failed to read body"})
			return
		}
		var req struct {
			MatchID string `json:"match_id"`
		}
		_ = json.Unmarshal(body, &req)
		target := p.pickUpstream(req.MatchID)
		if target == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no upstream primary available"})
			return
		}
		p.forwardJSON(w, r.Context(), http.MethodPost, "http://"+target+"/api/rooms", body)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
	}
}

// HandleMatchmake serves POST /api/matchmake by checking aggregated rooms for an
// open seat first, or forwarding to a primary pod to spin up a new match.
func (p *Proxy) HandleMatchmake(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	rooms, err := p.fetchAllRooms(r.Context())
	if err == nil {
		var fallback *RoomSummary
		for i := range rooms {
			room := &rooms[i]
			if room.Status != "in_progress" || len(room.OpenSeats) == 0 {
				continue
			}
			if len(room.OccupiedSeats) > 0 {
				writeJSON(w, http.StatusOK, map[string]any{
					"match_id":  room.MatchID,
					"player_id": room.OpenSeats[0],
					"created":   false,
				})
				return
			}
			if fallback == nil {
				fallback = room
			}
		}
		if fallback != nil {
			writeJSON(w, http.StatusOK, map[string]any{
				"match_id":  fallback.MatchID,
				"player_id": fallback.OpenSeats[0],
				"created":   false,
			})
			return
		}
	}

	target := p.pickUpstream("")
	if target == "" {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "no upstream primary available"})
		return
	}
	p.forwardJSON(w, r.Context(), http.MethodPost, "http://"+target+"/api/matchmake", []byte("{}"))
}

func (p *Proxy) upstreamAddrs() []string {
	seen := make(map[string]struct{})
	var addrs []string
	for _, rt := range p.table.Snapshot() {
		if rt.Addr != "" {
			if _, ok := seen[rt.Addr]; !ok {
				seen[rt.Addr] = struct{}{}
				addrs = append(addrs, rt.Addr)
			}
		}
	}
	if def := p.table.Default(); def != "" {
		if _, ok := seen[def]; !ok {
			addrs = append(addrs, def)
		}
	}
	sort.Strings(addrs)
	return addrs
}

func (p *Proxy) pickUpstream(matchID string) string {
	if matchID != "" {
		if rt, ok := p.table.Lookup(matchID); ok && rt.Addr != "" {
			return rt.Addr
		}
	}
	if def := p.table.Default(); def != "" {
		return def
	}
	addrs := p.upstreamAddrs()
	if len(addrs) > 0 {
		return addrs[0]
	}
	return ""
}

func (p *Proxy) fetchAllRooms(ctx context.Context) ([]RoomSummary, error) {
	addrs := p.upstreamAddrs()
	if len(addrs) == 0 {
		return []RoomSummary{}, nil
	}

	byMatch := make(map[string]RoomSummary)
	var lastErr error
	succeeded := 0

	for _, addr := range addrs {
		rooms, err := p.fetchRoomsFrom(ctx, addr)
		if err != nil {
			lastErr = err
			continue
		}
		succeeded++
		for _, rm := range rooms {
			existing, ok := byMatch[rm.MatchID]
			if !ok || rm.Sequence > existing.Sequence || rm.ActiveConnections > existing.ActiveConnections {
				byMatch[rm.MatchID] = rm
			}
		}
	}

	if succeeded == 0 && lastErr != nil {
		return nil, lastErr
	}

	out := make([]RoomSummary, 0, len(byMatch))
	for _, rm := range byMatch {
		out = append(out, rm)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].MatchID < out[j].MatchID
	})
	return out, nil
}

func (p *Proxy) fetchRoomsFrom(ctx context.Context, addr string) ([]RoomSummary, error) {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+addr+"/api/rooms", nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, http.ErrNotSupported
	}
	var env roomsEnvelope
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&env); err != nil {
		return nil, err
	}
	return env.Rooms, nil
}

func (p *Proxy) forwardJSON(w http.ResponseWriter, ctx context.Context, method, url string, body []byte) {
	reqCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, method, url, bytes.NewReader(body))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	defer func() { _ = resp.Body.Close() }()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(resp.StatusCode)
	_, _ = w.Write(respBody)
}
