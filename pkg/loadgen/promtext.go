package loadgen

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
)

// Sample is one Prometheus text-format sample.
type Sample struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// Metrics is a parsed /metrics scrape, keyed by metric name.
type Metrics map[string][]Sample

// ParseText parses the Prometheus text exposition format (version 0.0.4),
// which is what promhttp serves by default. It is deliberately small: the
// load probe only needs counters, gauges and histogram buckets.
func ParseText(r io.Reader) (Metrics, error) {
	out := Metrics{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 4<<20)
	for lineNo := 1; sc.Scan(); lineNo++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || line[0] == '#' {
			continue
		}
		s, err := parseLine(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNo, err)
		}
		out[s.Name] = append(out[s.Name], s)
	}
	return out, sc.Err()
}

func parseLine(line string) (Sample, error) {
	s := Sample{Labels: map[string]string{}}
	i := strings.IndexAny(line, "{ \t")
	if i < 0 {
		return s, fmt.Errorf("no value in %q", line)
	}
	s.Name = line[:i]
	rest := line[i:]
	if rest[0] == '{' {
		end, err := parseLabels(rest, s.Labels)
		if err != nil {
			return s, err
		}
		rest = rest[end:]
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, fmt.Errorf("no value in %q", line)
	}
	v, err := parseFloat(fields[0])
	if err != nil {
		return s, err
	}
	s.Value = v
	return s, nil
}

// parseLabels parses `{a="x",b="y"}` at the start of s and returns the index
// just past the closing brace.
func parseLabels(s string, into map[string]string) (int, error) {
	i := 1
	for {
		for i < len(s) && (s[i] == ' ' || s[i] == ',') {
			i++
		}
		if i >= len(s) {
			return 0, fmt.Errorf("unterminated labels")
		}
		if s[i] == '}' {
			return i + 1, nil
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			return 0, fmt.Errorf("bad label in %q", s)
		}
		key := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		if i >= len(s) || s[i] != '"' {
			return 0, fmt.Errorf("label %s: value not quoted", key)
		}
		i++
		var b strings.Builder
		for ; i < len(s) && s[i] != '"'; i++ {
			if s[i] == '\\' && i+1 < len(s) {
				i++
				switch s[i] {
				case 'n':
					b.WriteByte('\n')
				default:
					b.WriteByte(s[i])
				}
				continue
			}
			b.WriteByte(s[i])
		}
		if i >= len(s) {
			return 0, fmt.Errorf("label %s: unterminated value", key)
		}
		i++ // closing quote
		into[key] = b.String()
	}
}

func parseFloat(v string) (float64, error) {
	switch v {
	case "+Inf":
		return math.Inf(1), nil
	case "-Inf":
		return math.Inf(-1), nil
	case "NaN":
		return math.NaN(), nil
	}
	return strconv.ParseFloat(v, 64)
}

// Sum adds every sample of name whose labels include all of match.
func (m Metrics) Sum(name string, match map[string]string) float64 {
	total := 0.0
	for _, s := range m[name] {
		if labelsMatch(s.Labels, match) {
			total += s.Value
		}
	}
	return total
}

// Has reports whether the scrape contains name.
func (m Metrics) Has(name string) bool { return len(m[name]) > 0 }

func labelsMatch(have, want map[string]string) bool {
	for k, v := range want {
		if have[k] != v {
			return false
		}
	}
	return true
}

// Histogram is a cumulative bucket histogram keyed by upper bound.
type Histogram map[float64]float64

// Histogram merges <name>_bucket samples (summing across other labels).
func (m Metrics) Histogram(name string) Histogram {
	h := Histogram{}
	for _, s := range m[name+"_bucket"] {
		le, err := parseFloat(s.Labels["le"])
		if err != nil {
			continue
		}
		h[le] += s.Value
	}
	return h
}

// Sub returns h - prev bucket by bucket (the observations made in between).
func (h Histogram) Sub(prev Histogram) Histogram {
	out := Histogram{}
	for le, v := range h {
		d := v - prev[le]
		if d < 0 { // counter reset (pod restarted): use the new value
			d = v
		}
		out[le] = d
	}
	return out
}

// Add merges other into h.
func (h Histogram) Add(other Histogram) {
	for le, v := range other {
		h[le] += v
	}
}

// Count is the number of observations (the +Inf bucket).
func (h Histogram) Count() float64 { return h[math.Inf(1)] }

// Quantile estimates the q-quantile with the same linear interpolation as
// PromQL's histogram_quantile. It returns NaN for an empty histogram.
func (h Histogram) Quantile(q float64) float64 {
	if len(h) == 0 {
		return math.NaN()
	}
	bounds := make([]float64, 0, len(h))
	for le := range h {
		bounds = append(bounds, le)
	}
	sort.Float64s(bounds)
	total := h[bounds[len(bounds)-1]]
	if total <= 0 {
		return math.NaN()
	}
	rank := q * total
	prevBound, prevCount := 0.0, 0.0
	for _, le := range bounds {
		c := h[le]
		if c >= rank {
			if math.IsInf(le, 1) {
				return prevBound // above the highest finite bucket
			}
			if c == prevCount {
				return le
			}
			return prevBound + (le-prevBound)*(rank-prevCount)/(c-prevCount)
		}
		prevBound, prevCount = le, c
	}
	return prevBound
}
