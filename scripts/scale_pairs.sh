#!/usr/bin/env bash
set -euo pipefail

# Scale both ARCADE gameserver StatefulSets (arcade-gs-a and arcade-gs-b) in
# lockstep so every match-i retains a primary/standby pair by ordinal.
#
# Usage:
#   ./scripts/scale_pairs.sh <pair-count> [namespace]

PAIRS="${1:-}"
NAMESPACE="${2:-arcade}"

if [[ ! "${PAIRS}" =~ ^[0-9]+$ ]] || [[ "${PAIRS}" -lt 1 ]]; then
  echo "Usage: $0 <pair-count >= 1> [namespace]" >&2
  exit 1
fi

echo "[ARCADE] Scaling arcade-gs-a and arcade-gs-b to ${PAIRS} pairs in namespace '${NAMESPACE}'..."
kubectl -n "${NAMESPACE}" scale statefulset/arcade-gs-a statefulset/arcade-gs-b --replicas="${PAIRS}"

kubectl -n "${NAMESPACE}" rollout status statefulset/arcade-gs-a --timeout=120s
kubectl -n "${NAMESPACE}" rollout status statefulset/arcade-gs-b --timeout=120s

echo "[ARCADE] Scaled to ${PAIRS} active primary/standby pairs."
