#!/usr/bin/env bash
set -e

# Automatically resolve the repository root directory
REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null || pwd)"
cd "$REPO_ROOT"

SESSION="arcade-demo"

# 1. Sanity check: Ensure kubectl can reach the cluster before launching tmux
if ! kubectl get nodes >/dev/null 2>&1; then
  echo "ERROR: kubectl cannot communicate with a Kubernetes cluster."
  echo "Current context: $(kubectl config current-context 2>/dev/null || echo 'None')"
  echo "Run: 'kubectl config get-contexts' and 'kubectl config use-context <name>' first."
  exit 1
fi

# 2. Reset session
tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$REPO_ROOT"

# Clean styling
tmux set -t "$SESSION" status off
tmux set -t "$SESSION" pane-border-style "fg=colour238"
tmux set -t "$SESSION" pane-active-border-style "fg=colour39"

# 3. Split 2x2 grid (all keeping -c $REPO_ROOT)
tmux split-window -v -t "$SESSION:0" -c "$REPO_ROOT"
tmux split-window -h -t "$SESSION:0.0" -c "$REPO_ROOT"
tmux split-window -h -t "$SESSION:0.2" -c "$REPO_ROOT"

# 4. Stream Monitors
tmux send-keys -t "$SESSION:0.1" "kubectl -n arcade logs -f deploy/arcade-proxy --since=2s | grep --line-buffered -E 'route updated|moved to new primary'" C-m
tmux send-keys -t "$SESSION:0.2" "kubectl -n arcade logs -f arcade-gs-b-0 --since=2s | grep --line-buffered -E 'Dual-validation|heartbeat|Takeover|epoch|route'" C-m

# 5. Pre-type action commands from repo root
tmux send-keys -t "$SESSION:0.0" "go run ./cmd/wsclient -url ws://127.0.0.1:8080/ws -clients 5 -players-per-match 5 -match-prefix match-00 -duration 30s"
tmux send-keys -t "$SESSION:0.3" "go run ./cmd/chaos -mode k8s-crash -pair 0"

# 6. Focus Top-Left pane and attach
tmux select-pane -t "$SESSION:0.0"
tmux attach-session -t "$SESSION"
