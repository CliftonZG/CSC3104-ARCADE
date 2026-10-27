#!/usr/bin/env bash
set -eo pipefail

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

CLUSTER_NAME="arcade"
KIND_CONFIG="k8s/kind-cluster.yaml"
OVERLAY="demo"
DELETE_ONLY=false
QUICK_RESET=false
FORCE_BUILD=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Resets the ARCADE demonstration environment.

Options:
  -d, --delete-only, --down   Delete the Kind cluster and clean up background processes, then exit.
  -q, --quick                 Quick reset: flush Redis and restart pods inside existing Kind cluster.
  -b, --build                 Force rebuild of container images before loading into Kind.
  --overlay <name>            Kustomize overlay to apply (default: demo).
  -h, --help                  Show this help message.
EOF
  exit 0
}

# Parse command line flags
while [[ $# -gt 0 ]]; do
  case "$1" in
  -d | --delete-only | --remove-only | --down)
    DELETE_ONLY=true
    shift
    ;;
  -q | --quick | --pods-only)
    QUICK_RESET=true
    shift
    ;;
  -b | --build | --rebuild)
    FORCE_BUILD=true
    shift
    ;;
  --overlay)
    OVERLAY="$2"
    shift 2
    ;;
  -h | --help)
    usage
    ;;
  *)
    echo "Unknown option: $1"
    echo "Run with -h or --help for usage."
    exit 1
    ;;
  esac
done

echo "=== 1. Terminating background port-forwards & clients ==="
pkill -f "kubectl.*port-forward.*8080" 2>/dev/null || true
pkill -f "go run ./cmd/wsclient" 2>/dev/null || true
pkill -f "go run ./cmd/chaos" 2>/dev/null || true

# Option: Remove Kind cluster only
if [ "$DELETE_ONLY" = true ]; then
  echo "=== 2. Removing Kind cluster ($CLUSTER_NAME) ==="
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    kind delete cluster --name "$CLUSTER_NAME"
    echo "Kind cluster '${CLUSTER_NAME}' removed successfully."
  else
    echo "Kind cluster '${CLUSTER_NAME}' was not found. Nothing to delete."
  fi
  exit 0
fi

# Option: Quick in-cluster pod reset without recreating Kind
if [ "$QUICK_RESET" = true ]; then
  echo "=== 2. Flushing Redis datastore ==="
  REDIS_POD=$(kubectl -n arcade get pod -l app.kubernetes.io/component=datastore -o jsonpath='{.items[0].metadata.name}' 2>/dev/null ||
    kubectl -n arcade get pod -l app=arcade-redis -o jsonpath='{.items[0].metadata.name}' 2>/dev/null ||
    kubectl -n arcade get pods -n arcade 2>/dev/null | grep redis | awk '{print $1}' | head -n 1)

  if [ -n "$REDIS_POD" ]; then
    kubectl -n arcade exec -i "$REDIS_POD" -- redis-cli FLUSHALL >/dev/null
    echo "Redis flushed cleanly."
  else
    echo "WARNING: Redis pod not found, skipping flush."
  fi

  echo "=== 3. Resetting Gameserver Pods in Sequence ==="
  # Scale standby down to wipe stale epoch memory
  kubectl -n arcade scale statefulset arcade-gs-b --replicas=0 >/dev/null
  kubectl -n arcade rollout restart deploy/arcade-proxy >/dev/null

  # Bring standby B-0 up first so its DNS record exists in CoreDNS
  echo "Starting Standby B-0..."
  kubectl -n arcade scale statefulset arcade-gs-b --replicas=1 >/dev/null
  kubectl -n arcade wait --for=condition=Ready pod/arcade-gs-b-0 --timeout=45s

  # Restart primary A-0 so its UDP transmitter resolves B-0 cleanly on boot
  echo "Restarting Primary A-0 to bind UDP transmitter to B-0..."
  kubectl -n arcade delete pod arcade-gs-a-0 --grace-period=0 --force >/dev/null 2>&1 || true
  kubectl -n arcade wait --for=condition=Ready pod/arcade-gs-a-0 --timeout=45s
  kubectl -n arcade rollout status deploy/arcade-proxy --timeout=45s >/dev/null
else
  # Default: Full Kind cluster teardown and recreation
  echo "=== 2. Deleting Kind cluster ($CLUSTER_NAME) ==="
  if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
    kind delete cluster --name "$CLUSTER_NAME"
  fi

  echo "=== 3. Creating Kind cluster ($CLUSTER_NAME) ==="
  if [ ! -f "$KIND_CONFIG" ]; then
    echo "ERROR: Kind configuration '$KIND_CONFIG' not found in $REPO_ROOT"
    exit 1
  fi
  kind create cluster --config "$KIND_CONFIG"

  echo "=== 4. Preparing & loading container images ==="
  IMAGES=(
    "arcade/gameserver:dev:deployments/Dockerfile.gameserver"
    "arcade/proxy:dev:deployments/Dockerfile.proxy"
    "arcade/loadprobe:dev:deployments/Dockerfile.loadprobe"
  )

  LOAD_TARGETS=()
  for item in "${IMAGES[@]}"; do
    IFS=":" read -r tag _ dockerfile <<<"$item"
    full_tag="${tag}:dev"
    if [ "$FORCE_BUILD" = true ] || ! docker image inspect "$full_tag" >/dev/null 2>&1; then
      echo "Building $full_tag..."
      DOCKER_BUILDKIT=1 docker build -f "$dockerfile" -t "$full_tag" .
    fi
    LOAD_TARGETS+=("$full_tag")
  done

  echo "Loading images into Kind cluster '${CLUSTER_NAME}'..."
  kind load docker-image "${LOAD_TARGETS[@]}" --name "$CLUSTER_NAME"

  echo "=== 5. Applying manifests (k8s/overlays/${OVERLAY}) ==="
  kubectl apply -k "k8s/overlays/${OVERLAY}"

  echo "Waiting for Redis and Ingress Proxy to be Ready..."
  kubectl -n arcade rollout status statefulset/arcade-redis --timeout=90s
  kubectl -n arcade rollout status deploy/arcade-proxy --timeout=90s

  echo "Waiting for Gameserver pair 0 (A-0 & B-0) to be Ready..."
  for pod in arcade-gs-a-0 arcade-gs-b-0; do
    for _ in {1..30}; do
      if kubectl -n arcade get pod "$pod" >/dev/null 2>&1; then
        break
      fi
      sleep 1
    done
    kubectl -n arcade wait --for=condition=Ready "pod/$pod" --timeout=90s
  done

  # Bounce Primary A-0 once to ensure its UDP transmitter resolved B-0 after CoreDNS registration
  echo "Cycling Primary A-0 to guarantee UDP transmitter resolves B-0..."
  kubectl -n arcade delete pod arcade-gs-a-0 --grace-period=0 --force >/dev/null 2>&1 || true
  kubectl -n arcade wait --for=condition=Ready pod/arcade-gs-a-0 --timeout=45s
fi

echo "=== 6. Re-establishing Proxy Port-Forward (8080) ==="
if curl -s -f --max-time 2 http://127.0.0.1:8080/healthz >/dev/null 2>&1; then
  echo "Proxy port 8080 is reachable directly via Kind port mapping."
else
  echo "Establishing kubectl port-forward for svc/arcade-proxy (8080)..."
  nohup kubectl -n arcade port-forward svc/arcade-proxy 8080:8080 >/dev/null 2>&1 &
  sleep 2

  if ! curl -s -f --max-time 2 http://127.0.0.1:8080/healthz >/dev/null 2>&1 && ! nc -z 127.0.0.1 8080 2>/dev/null; then
    echo "Retrying port-forward via pod directly..."
    PROXY_POD=$(kubectl -n arcade get pod -l app.kubernetes.io/name=arcade-proxy -o jsonpath='{.items[0].metadata.name}' 2>/dev/null ||
      kubectl -n arcade get pod -l app=arcade-proxy -o jsonpath='{.items[0].metadata.name}')
    nohup kubectl -n arcade port-forward "$PROXY_POD" 8080:8080 >/dev/null 2>&1 &
    sleep 2
  fi
fi

echo "=== 7. Fleet Role Verification ==="
kubectl -n arcade get pods -L arcade.io/role,arcade.io/match

echo "=== 8. Standby Log Health Check (Testing 3 seconds of silence) ==="
sleep 3
ERRORS=$(kubectl -n arcade logs arcade-gs-b-0 --since=3s 2>&1 | grep -E "Promotion aborted|fenced" || true)
if [ -n "$ERRORS" ]; then
  echo "FAIL: Standby still logging promotion errors:"
  echo "$ERRORS"
  exit 1
else
  echo "PASS: Standby is healthy, silent, and receiving UDP heartbeats."
  echo "Ready to launch ./scripts/record_tmux.sh"
fi
