package loadgen

import (
	"math"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const sample = `# HELP process_cpu_seconds_total Total user and system CPU time spent in seconds.
# TYPE process_cpu_seconds_total counter
process_cpu_seconds_total 1.25
process_resident_memory_bytes 1.2582912e+07
arcade_loadgen_moves_total{outcome="ok"} 40
arcade_loadgen_moves_total{outcome="rejected"} 3
weird{path="a\"b,c}",le="1"} 2
lat_bucket{format="json",le="1"} 10
lat_bucket{format="json",le="2"} 30
lat_bucket{format="json",le="+Inf"} 40
lat_bucket{format="binary",le="1"} 0
lat_bucket{format="binary",le="2"} 0
lat_bucket{format="binary",le="+Inf"} 0
`

func TestParseText(t *testing.T) {
	m, err := ParseText(strings.NewReader(sample))
	if err != nil {
		t.Fatal(err)
	}
	if v := m.Sum("process_cpu_seconds_total", nil); v != 1.25 {
		t.Fatalf("cpu = %v", v)
	}
	if v := m.Sum("process_resident_memory_bytes", nil); v != 12582912 {
		t.Fatalf("rss = %v", v)
	}
	if v := m.Sum("arcade_loadgen_moves_total", map[string]string{"outcome": "ok"}); v != 40 {
		t.Fatalf("ok moves = %v", v)
	}
	if v := m.Sum("arcade_loadgen_moves_total", nil); v != 43 {
		t.Fatalf("all moves = %v", v)
	}
	if got := m["weird"][0].Labels["path"]; got != `a"b,c}` {
		t.Fatalf("escaped label = %q", got)
	}
	h := m.Histogram("lat")
	if h.Count() != 40 {
		t.Fatalf("count = %v", h.Count())
	}
	// rank 20 of 40 lies halfway through the (1,2] bucket (10 -> 30).
	if q := h.Quantile(0.5); math.Abs(q-1.5) > 1e-9 {
		t.Fatalf("p50 = %v", q)
	}
	// rank 39.6 is beyond the last finite bucket: report its bound.
	if q := h.Quantile(0.99); q != 2 {
		t.Fatalf("p99 = %v", q)
	}
}

func TestHistogramSubAndEmpty(t *testing.T) {
	prev := Histogram{1: 5, 2: 10, math.Inf(1): 10}
	cur := Histogram{1: 5, 2: 20, math.Inf(1): 25}
	d := cur.Sub(prev)
	if d.Count() != 15 || d[1] != 0 || d[2] != 10 {
		t.Fatalf("delta = %v", d)
	}
	if !math.IsNaN((Histogram{}).Quantile(0.5)) {
		t.Fatal("empty histogram quantile should be NaN")
	}
}

// The parser must understand what promhttp actually serves.
func TestParseRealPromhttpOutput(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}), collectors.NewGoCollector())
	h := prometheus.NewHistogram(prometheus.HistogramOpts{Name: "x_seconds", Buckets: []float64{.001, .01}})
	reg.MustRegister(h)
	h.Observe(.005)
	h.Observe(.02)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	req.Header.Set("Accept", "text/plain; version=0.0.4")
	promhttp.HandlerFor(reg, promhttp.HandlerOpts{}).ServeHTTP(rec, req)

	m, err := ParseText(rec.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !m.Has("go_goroutines") || !m.Has("go_memstats_heap_inuse_bytes") {
		t.Fatal("go collector metrics missing")
	}
	if got := m.Histogram("x_seconds").Count(); got != 2 {
		t.Fatalf("histogram count = %v", got)
	}
}
