package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TurnExecutionLatency tracks turn execution latency in microseconds.
	TurnExecutionLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "uno",
			Subsystem: "engine",
			Name:      "turn_execution_latency_us",
			Help:      "Latency of state transition execution in microseconds.",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
		},
		[]string{"action_type"},
	)

	// SnapshotSyncDuration tracks snapshot serialization and replication duration.
	SnapshotSyncDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "uno",
			Subsystem: "replication",
			Name:      "snapshot_sync_duration_ms",
			Help:      "Duration of snapshot serialization and Redis persistence in milliseconds.",
			Buckets:   []float64{0.1, 0.5, 1, 2, 5, 10, 25, 50, 100},
		},
		[]string{"format"}, // "json" or "binary"
	)

	// SnapshotSizeBytes tracks the wire size of state snapshots.
	SnapshotSizeBytes = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "uno",
			Subsystem: "replication",
			Name:      "snapshot_size_bytes",
			Help:      "Payload size of serialized state snapshots in bytes.",
			Buckets:   []float64{100, 250, 500, 1000, 2000, 4000, 8000},
		},
		[]string{"format"},
	)

	// FailoverDowntime tracks end-to-end failover latency from crash to resumed turn.
	FailoverDowntime = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "uno",
			Subsystem: "cluster",
			Name:      "failover_downtime_ms",
			Help:      "End-to-end failover downtime from primary failure to backup serving traffic in ms.",
			Buckets:   []float64{50, 100, 200, 300, 500, 750, 1000, 1500, 2000},
		},
	)

	// ActiveMatches tracks the number of concurrently running Uno games.
	ActiveMatches = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "uno",
			Subsystem: "fleet",
			Name:      "active_matches_total",
			Help:      "Total number of active Uno matches currently hosted on this pod.",
		},
	)

	// TotalTurnsCommitted counts total valid moves executed.
	TotalTurnsCommitted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "uno",
			Subsystem: "engine",
			Name:      "turns_committed_total",
			Help:      "Total count of successfully committed turns.",
		},
		[]string{"action_type"},
	)
)
