# CSC3104 ARCADE — Fault-Tolerant Distributed Uno

ARCADE is a cloud-native, fault-tolerant distributed Uno game platform featuring active-standby pod replication, dual-validation failure detection, Redis Lua fencing, dynamic WebSocket proxy retargeting, and automated chaos testing.

---

## Project Structure (Standard Go Monorepo)

```
CSC3104-ARCADE/
├── .github/
│   └── workflows/
│       ├── ci.yml              # CI: Linting, Race Tests, Fuzzing, Benchmarks, Cross-Builds
│       └── cd.yml              # CD: Multi-Arch Buildx & GHCR Container Publishing
├── cmd/
│   ├── gameserver/             # Primary & Standby game server executable (Tan Yan Ting / Clifton)
│   │   └── main.go
│   ├── proxy/                  # Client-facing WebSocket routing proxy (Brian)
│   │   └── main.go
│   └── chaos/                  # Automated fault-injection CLI (Sam)
│       └── main.go
├── pkg/
│   ├── uno/                    # Pure Uno state machine, snapshots, invariants (Clifton)
│   │   ├── action.go           # Command models & validation
│   │   ├── binary.go           # Bit-packed 1-byte card codec (523ns, 176B)
│   │   ├── card.go             # Card definitions & deck constructor
│   │   ├── checksum.go         # SHA-256 deterministic fingerprinting
│   │   ├── dispatcher.go       # Atomic turn transitions & error rollback
│   │   ├── engine.go           # Thread-safe Game engine (sync.RWMutex)
│   │   ├── errors.go           # Centralized domain errors
│   │   ├── event.go            # Domain events & ActionSummary compilation
│   │   ├── fuzz_test.go        # Continuous Go native fuzz testing
│   │   ├── game.go             # Base Uno rules & turn sequence
│   │   ├── hook.go             # Thread-safe MoveHook observer registry
│   │   ├── pool.go             # sync.Pool buffer recycling (0 GC stalls)
│   │   ├── restore.go          # State deserialization & reader streaming
│   │   ├── snapshot.go         # Hot serialization & zero-copy streaming
│   │   ├── state.go            # Immutable State struct & cloning
│   │   └── validator.go        # 108-card conservation law invariant verification
│   ├── replication/            # Redis snapshot streaming & Lua fencing (Benny)
│   ├── detector/               # UDP heartbeat & dual-validation timeout monitor (Marcus)
│   ├── proxy/                  # Dynamic upstream session router (Brian)
│   ├── protocol/               # Shared wire envelopes, actions & events
│   │   └── envelope.go
│   └── metrics/                # Prometheus metrics collectors (Sam)
│       └── metrics.go
├── deployments/                # Multi-stage distroless Docker configurations (Tan Yan Ting)
│   ├── Dockerfile.gameserver
│   └── Dockerfile.proxy
├── k8s/                        # Kubernetes manifests & Headless Services (Tan Yan Ting)
├── test/                       # End-to-end integration and load testing suites
│   ├── e2e/                    # Failover chaos verification
│   └── load/                   # Concurrent match scaling tests
├── .golangci.yml               # Strict linter configuration
├── Makefile                    # Build, test, race, bench automation
└── go.mod                      # Root module: github.com/CSC3104-ARCADE
```

---

## Continuous Integration & Continuous Delivery (CI/CD)

The repository includes enterprise-grade GitHub Actions workflows:

### CI Pipeline (`.github/workflows/ci.yml`)
Runs on every push and pull request to `main`:
1. **Lint & Code Quality:** Runs `gofmt` verification, `go vet`, and `golangci-lint` (errcheck, staticcheck, gocyclo).
2. **Race-Condition Testing:** Executes `go test -v -race ./...` with code coverage tracking.
3. **Continuous Fuzz Testing:** Executes 30s automated fuzzing on both JSON and binary snapshot decoders to guarantee immunity against corrupted network payloads.
4. **Performance Regression Benchmarks:** Measures serialization throughput and memory allocations.
5. **Cross-Platform Compilation:** Compiles static binaries for `linux/amd64`, `linux/arm64`, `darwin/amd64`, and `darwin/arm64`.

### CD Pipeline (`.github/workflows/cd.yml`)
Runs on release tags (`v*`) and merge to `main`:
1. **Multi-Architecture Builds:** Uses Docker Buildx to cross-compile container images for `linux/amd64` and `linux/arm64`.
2. **Hardened Distroless Images:** Uses `gcr.io/distroless/static-debian12:nonroot` for minimal attack surface (<25MB) and sub-second container cold-starts.
3. **Registry Publishing:** Pushes immutable images to GitHub Container Registry (`ghcr.io`).

---

## Developer Workflows

```bash
# Build all binaries
make build

# Run test suite with race detector (0 data races)
make race

# Run throughput & allocation benchmarks
make bench

# Start gameserver locally with Prometheus & pprof enabled (:8081)
make run-server
```
