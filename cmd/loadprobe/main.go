// Command loadprobe measures the CPU and memory footprint of every
// Primary/Standby pair in the ARCADE fleet, plus throughput and latency,
// over a fixed measurement window.
//
// It runs inside the cluster as a Kubernetes Job (k8s/load/loadprobe-job.yaml)
// and reaches each gameserver through its stable headless-Service DNS name,
// so it needs no kubectl, port-forwarding or metrics-server. For each pod it
// scrapes /metrics twice, `-window` apart, and derives:
//
//   - CPU cores used     = Δ process_cpu_seconds_total / Δt
//   - resident memory    = process_resident_memory_bytes (RSS)
//   - Go heap in use     = go_memstats_heap_inuse_bytes
//   - move throughput    = Δ arcade_loadgen_moves_total{outcome="ok"} / Δt
//   - move latency       = quantiles of Δ arcade_loadgen_apply_latency_seconds
//   - Redis commit time  = quantiles of Δ uno_replication_snapshot_sync_duration_ms
//
// With -redis it also samples the shared Redis (memory, CPU, ops/s), the
// one component every pair shares and therefore the likely bottleneck.
//
// Output: a human-readable table, then one JSON document between the
// BEGIN/END markers below (test/load/run-load.ps1 extracts it).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CSC3104-ARCADE/pkg/fleet"
	"github.com/CSC3104-ARCADE/pkg/loadgen"
	"github.com/redis/go-redis/v9"
)

const (
	jsonBegin = "=== LOADPROBE JSON BEGIN ==="
	jsonEnd   = "=== LOADPROBE JSON END ==="
)

type target struct {
	Pod  string
	Pair int
	Set  string
	Addr string // host:port
}

type scrape struct {
	at  time.Time
	m   loadgen.Metrics
	err error
}

// PodResult is the measurement for one pod over the window.
type PodResult struct {
	Pod         string  `json:"pod"`
	Pair        int     `json:"pair"`
	Role        string  `json:"role"`
	Reachable   bool    `json:"reachable"`
	Error       string  `json:"error,omitempty"`
	CPUMilli    float64 `json:"cpu_millicores"`
	RSSMiB      float64 `json:"rss_mib"`
	HeapMiB     float64 `json:"heap_inuse_mib"`
	Goroutines  float64 `json:"goroutines"`
	MovesPerSec float64 `json:"moves_per_sec"`
	Restarted   bool    `json:"restarted,omitempty"`
}

// PairResult sums the two pods of a pair.
type PairResult struct {
	Pair     int     `json:"pair"`
	Complete bool    `json:"complete"` // both pods reachable
	CPUMilli float64 `json:"cpu_millicores"`
	RSSMiB   float64 `json:"rss_mib"`
}

// Stats summarises a distribution across pairs or pods.
type Stats struct {
	Mean float64 `json:"mean"`
	P50  float64 `json:"p50"`
	P95  float64 `json:"p95"`
	Max  float64 `json:"max"`
	Sum  float64 `json:"sum"`
}

// RedisResult describes the shared Redis over the window.
type RedisResult struct {
	Addr         string  `json:"addr"`
	UsedMemMiB   float64 `json:"used_memory_mib"`
	CPUMilli     float64 `json:"cpu_millicores"`
	OpsPerSec    float64 `json:"ops_per_sec"`
	Clients      float64 `json:"connected_clients"`
	KeyspaceKeys float64 `json:"keys"`
	Error        string  `json:"error,omitempty"`
}

// Report is the JSON document the probe emits.
type Report struct {
	Label          string    `json:"label"`
	StartedAt      time.Time `json:"started_at"`
	WindowSec      float64   `json:"window_seconds"`
	PairsRequested int       `json:"pairs_requested"`
	PairsComplete  int       `json:"pairs_complete"`
	PodsReachable  int       `json:"pods_reachable"`
	Primaries      int       `json:"primaries"`

	PairCPUMilli Stats `json:"pair_cpu_millicores"`
	PairRSSMiB   Stats `json:"pair_rss_mib"`

	PrimaryCPUMilli Stats `json:"primary_cpu_millicores"`
	BackupCPUMilli  Stats `json:"backup_cpu_millicores"`
	PrimaryRSSMiB   Stats `json:"primary_rss_mib"`
	BackupRSSMiB    Stats `json:"backup_rss_mib"`

	TotalMovesPerSec float64 `json:"total_moves_per_sec"`
	MoveLatencyMs    Quant   `json:"move_latency_ms"`
	RedisCommitMs    Quant   `json:"redis_commit_ms"`

	Redis *RedisResult `json:"redis,omitempty"`
	Pods  []PodResult  `json:"pods"`
	Pairs []PairResult `json:"pairs"`
}

// Quant holds latency quantiles.
type Quant struct {
	Count float64 `json:"count"`
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
}

func main() {
	var (
		namespace   = flag.String("namespace", envOr("POD_NAMESPACE", "arcade"), "namespace of the gameserver pods")
		service     = flag.String("service", "arcade-gs", "governing headless Service")
		sets        = flag.String("sets", "arcade-gs-a,arcade-gs-b", "the two StatefulSets forming the pairs")
		pairs       = flag.Int("pairs", 1, "number of pairs (StatefulSet replicas) to measure")
		port        = flag.Int("port", fleet.DefaultHTTPPort, "gameserver HTTP port")
		window      = flag.Duration("window", 60*time.Second, "measurement window")
		concurrency = flag.Int("concurrency", 32, "parallel scrapes")
		redisAddr   = flag.String("redis", "", "Redis address to sample (optional)")
		label       = flag.String("label", "", "free-text label stored in the report (e.g. pairs=50)")
		endpoints   = flag.String("endpoints", "", "explicit pod=host:port list (comma separated) instead of DNS; for local runs")
		domain      = flag.String("cluster-domain", fleet.DefaultClusterDomain, "cluster DNS domain")
	)
	flag.Parse()
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	targets, err := buildTargets(*endpoints, *sets, *service, *namespace, *domain, *pairs, *port)
	if err != nil {
		log.Fatalf("[LOADPROBE] %v", err)
	}
	if *label == "" {
		*label = fmt.Sprintf("pairs=%d", *pairs)
	}
	log.Printf("[LOADPROBE] %d pods (%d pairs), window %s", len(targets), *pairs, *window)

	client := &http.Client{Timeout: 10 * time.Second}
	ctx := context.Background()

	var rdb *redis.Client
	if *redisAddr != "" {
		rdb = redis.NewClient(&redis.Options{Addr: *redisAddr})
		defer func() { _ = rdb.Close() }()
	}

	started := time.Now()
	redis0, redisErr0 := redisInfo(ctx, rdb)
	first := scrapeAll(ctx, client, targets, *concurrency)
	log.Printf("[LOADPROBE] first scrape done in %s; waiting %s", time.Since(started).Round(time.Millisecond), *window)
	time.Sleep(*window)
	second := scrapeAll(ctx, client, targets, *concurrency)
	redis1, redisErr1 := redisInfo(ctx, rdb)

	rep := buildReport(*label, started, *pairs, targets, first, second)
	if rdb != nil {
		rep.Redis = buildRedis(*redisAddr, redis0, redis1, errors.Join(redisErr0, redisErr1))
	}

	printTable(os.Stdout, rep)
	fmt.Println(jsonBegin)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(rep); err != nil {
		log.Fatalf("[LOADPROBE] encode: %v", err)
	}
	fmt.Println(jsonEnd)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func buildTargets(endpoints, sets, service, ns, domain string, pairs, port int) ([]target, error) {
	if endpoints != "" {
		var out []target
		for _, item := range strings.Split(endpoints, ",") {
			pod, addr, ok := strings.Cut(strings.TrimSpace(item), "=")
			if !ok {
				return nil, fmt.Errorf("bad endpoint %q (want pod=host:port)", item)
			}
			pair := -1
			if i := strings.LastIndexByte(pod, '-'); i > 0 {
				if n, err := strconv.Atoi(pod[i+1:]); err == nil {
					pair = n
				}
			}
			out = append(out, target{Pod: pod, Pair: pair, Addr: addr})
		}
		return out, nil
	}
	names := strings.Split(sets, ",")
	if len(names) != 2 || pairs <= 0 {
		return nil, fmt.Errorf("need exactly two -sets and -pairs > 0")
	}
	var out []target
	for i := 0; i < pairs; i++ {
		for _, set := range names {
			pod := fmt.Sprintf("%s-%d", strings.TrimSpace(set), i)
			host := fleet.PodFQDN(pod, service, ns, domain)
			out = append(out, target{Pod: pod, Pair: i, Set: set, Addr: fmt.Sprintf("%s:%d", host, port)})
		}
	}
	return out, nil
}

func scrapeAll(ctx context.Context, client *http.Client, targets []target, workers int) []scrape {
	out := make([]scrape, len(targets))
	sem := make(chan struct{}, max(1, workers))
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		go func(i int, t target) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = scrapeOne(ctx, client, t)
		}(i, t)
	}
	wg.Wait()
	return out
}

func scrapeOne(ctx context.Context, client *http.Client, t target) scrape {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+t.Addr+"/metrics", nil)
	if err != nil {
		return scrape{err: err}
	}
	// Ask for the plain text format; the parser does not speak protobuf/OpenMetrics.
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	resp, err := client.Do(req)
	if err != nil {
		return scrape{err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	at := time.Now()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, resp.Body)
		return scrape{err: fmt.Errorf("%s: %s", t.Addr, resp.Status)}
	}
	m, err := loadgen.ParseText(resp.Body)
	return scrape{at: at, m: m, err: err}
}

func buildReport(label string, started time.Time, pairs int, targets []target, a, b []scrape) Report {
	rep := Report{Label: label, StartedAt: started.UTC(), PairsRequested: pairs}
	moveLat, redisLat := loadgen.Histogram{}, loadgen.Histogram{}
	pairAgg := map[int]*PairResult{}
	pairPods := map[int]int{}
	var windows []float64

	var primCPU, backCPU, primRSS, backRSS []float64
	for i, t := range targets {
		pr := PodResult{Pod: t.Pod, Pair: t.Pair}
		s0, s1 := a[i], b[i]
		switch {
		case s0.err != nil:
			pr.Error = s0.err.Error()
		case s1.err != nil:
			pr.Error = s1.err.Error()
		default:
			pr.Reachable = true
		}
		if pr.Reachable {
			dt := s1.at.Sub(s0.at).Seconds()
			windows = append(windows, dt)
			cpu0, cpu1 := s0.m.Sum("process_cpu_seconds_total", nil), s1.m.Sum("process_cpu_seconds_total", nil)
			if cpu1 < cpu0 {
				pr.Restarted = true
				cpu0 = 0
			}
			pr.CPUMilli = 1000 * (cpu1 - cpu0) / dt
			pr.RSSMiB = s1.m.Sum("process_resident_memory_bytes", nil) / (1 << 20)
			pr.HeapMiB = s1.m.Sum("go_memstats_heap_inuse_bytes", nil) / (1 << 20)
			pr.Goroutines = s1.m.Sum("go_goroutines", nil)
			ok := map[string]string{"outcome": "ok"}
			mv := s1.m.Sum("arcade_loadgen_moves_total", ok) - s0.m.Sum("arcade_loadgen_moves_total", ok)
			if mv < 0 {
				mv = s1.m.Sum("arcade_loadgen_moves_total", ok)
			}
			pr.MovesPerSec = mv / dt
			pr.Role = "backup"
			if s1.m.Sum("arcade_fleet_is_primary", nil) >= 1 {
				pr.Role = "primary"
				rep.Primaries++
				primCPU, primRSS = append(primCPU, pr.CPUMilli), append(primRSS, pr.RSSMiB)
			} else {
				backCPU, backRSS = append(backCPU, pr.CPUMilli), append(backRSS, pr.RSSMiB)
			}
			moveLat.Add(s1.m.Histogram("arcade_loadgen_apply_latency_seconds").Sub(s0.m.Histogram("arcade_loadgen_apply_latency_seconds")))
			redisLat.Add(s1.m.Histogram("uno_replication_snapshot_sync_duration_ms").Sub(s0.m.Histogram("uno_replication_snapshot_sync_duration_ms")))

			rep.PodsReachable++
			rep.TotalMovesPerSec += pr.MovesPerSec
			p := pairAgg[t.Pair]
			if p == nil {
				p = &PairResult{Pair: t.Pair}
				pairAgg[t.Pair] = p
			}
			p.CPUMilli += pr.CPUMilli
			p.RSSMiB += pr.RSSMiB
			pairPods[t.Pair]++
		}
		rep.Pods = append(rep.Pods, pr)
	}

	var pairCPU, pairRSS []float64
	keys := make([]int, 0, len(pairAgg))
	for k := range pairAgg {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	for _, k := range keys {
		p := pairAgg[k]
		p.Complete = pairPods[k] == 2
		rep.Pairs = append(rep.Pairs, *p)
		if p.Complete {
			rep.PairsComplete++
			pairCPU, pairRSS = append(pairCPU, p.CPUMilli), append(pairRSS, p.RSSMiB)
		}
	}
	rep.PairCPUMilli, rep.PairRSSMiB = stats(pairCPU), stats(pairRSS)
	rep.PrimaryCPUMilli, rep.BackupCPUMilli = stats(primCPU), stats(backCPU)
	rep.PrimaryRSSMiB, rep.BackupRSSMiB = stats(primRSS), stats(backRSS)
	rep.WindowSec = stats(windows).Mean
	rep.MoveLatencyMs = quant(moveLat, 1000)
	rep.RedisCommitMs = quant(redisLat, 1)
	return rep
}

func quant(h loadgen.Histogram, scale float64) Quant {
	q := Quant{Count: h.Count()}
	if q.Count == 0 {
		return q
	}
	q.P50 = round3(h.Quantile(0.50) * scale)
	q.P95 = round3(h.Quantile(0.95) * scale)
	q.P99 = round3(h.Quantile(0.99) * scale)
	return q
}

func stats(v []float64) Stats {
	if len(v) == 0 {
		return Stats{}
	}
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	sum := 0.0
	for _, x := range s {
		sum += x
	}
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(len(s)))) - 1
		return s[min(max(idx, 0), len(s)-1)]
	}
	return Stats{Mean: round3(sum / float64(len(s))), P50: round3(pct(.5)), P95: round3(pct(.95)), Max: round3(s[len(s)-1]), Sum: round3(sum)}
}

func round3(x float64) float64 {
	if math.IsNaN(x) || math.IsInf(x, 0) {
		return 0
	}
	return math.Round(x*1000) / 1000
}

type redisSample struct {
	at     time.Time
	fields map[string]float64
}

func redisInfo(ctx context.Context, rdb *redis.Client) (redisSample, error) {
	if rdb == nil {
		return redisSample{}, nil
	}
	c, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	raw, err := rdb.Info(c, "memory", "cpu", "stats", "clients", "keyspace").Result()
	if err != nil {
		return redisSample{}, err
	}
	f := map[string]float64{}
	for _, line := range strings.Split(raw, "\n") {
		k, v, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		if k == "db0" { // keys=12,expires=0,avg_ttl=0
			for _, kv := range strings.Split(v, ",") {
				if kk, vv, ok := strings.Cut(kv, "="); ok && kk == "keys" {
					f["db0_keys"], _ = strconv.ParseFloat(vv, 64)
				}
			}
			continue
		}
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			f[k] = n
		}
	}
	return redisSample{at: time.Now(), fields: f}, nil
}

func buildRedis(addr string, a, b redisSample, err error) *RedisResult {
	r := &RedisResult{Addr: addr}
	if err != nil {
		r.Error = err.Error()
		return r
	}
	dt := b.at.Sub(a.at).Seconds()
	if dt <= 0 {
		return r
	}
	cpu := func(s redisSample) float64 { return s.fields["used_cpu_sys"] + s.fields["used_cpu_user"] }
	r.UsedMemMiB = round3(b.fields["used_memory"] / (1 << 20))
	r.CPUMilli = round3(1000 * (cpu(b) - cpu(a)) / dt)
	r.OpsPerSec = round3((b.fields["total_commands_processed"] - a.fields["total_commands_processed"]) / dt)
	r.Clients = b.fields["connected_clients"]
	r.KeyspaceKeys = b.fields["db0_keys"]
	return r
}

func printTable(w io.Writer, r Report) {
	pf := func(format string, args ...any) { _, _ = fmt.Fprintf(w, format, args...) }
	pf("\nARCADE load probe  %s  (window %.1fs)\n", r.Label, r.WindowSec)
	pf("pairs: %d requested, %d complete; pods reachable: %d; primaries: %d\n",
		r.PairsRequested, r.PairsComplete, r.PodsReachable, r.Primaries)
	pf("%-22s %10s %10s %10s %10s\n", "", "mean", "p50", "p95", "max")
	row := func(name string, s Stats) {
		pf("%-22s %10.2f %10.2f %10.2f %10.2f\n", name, s.Mean, s.P50, s.P95, s.Max)
	}
	row("pair CPU (mcores)", r.PairCPUMilli)
	row("pair RSS (MiB)", r.PairRSSMiB)
	row("primary CPU (mcores)", r.PrimaryCPUMilli)
	row("backup CPU (mcores)", r.BackupCPUMilli)
	row("primary RSS (MiB)", r.PrimaryRSSMiB)
	row("backup RSS (MiB)", r.BackupRSSMiB)
	pf("fleet total: CPU %.0f mcores, RSS %.1f MiB, %.1f moves/s\n", r.PairCPUMilli.Sum, r.PairRSSMiB.Sum, r.TotalMovesPerSec)
	pf("move latency ms  p50 %.3f  p95 %.3f  p99 %.3f  (n=%.0f)\n", r.MoveLatencyMs.P50, r.MoveLatencyMs.P95, r.MoveLatencyMs.P99, r.MoveLatencyMs.Count)
	pf("redis commit ms  p50 %.3f  p95 %.3f  p99 %.3f  (n=%.0f)\n", r.RedisCommitMs.P50, r.RedisCommitMs.P95, r.RedisCommitMs.P99, r.RedisCommitMs.Count)
	if r.Redis != nil {
		if r.Redis.Error != "" {
			pf("redis: error %s\n", r.Redis.Error)
		} else {
			pf("redis: %.1f MiB used, %.0f mcores, %.0f ops/s, %.0f clients, %.0f keys\n",
				r.Redis.UsedMemMiB, r.Redis.CPUMilli, r.Redis.OpsPerSec, r.Redis.Clients, r.Redis.KeyspaceKeys)
		}
	}
	unreachable := 0
	for _, p := range r.Pods {
		if !p.Reachable {
			if unreachable < 5 {
				pf("  unreachable: %s (%s)\n", p.Pod, p.Error)
			}
			unreachable++
		}
	}
	if unreachable > 5 {
		pf("  ... and %d more unreachable\n", unreachable-5)
	}
	pf("\n")
}
