#!/usr/bin/env bash
#
# dev.sh
#
# One-command local startup for the whole DSView pipeline: brings up the
# docker-compose infra, waits until each service actually accepts
# connections (docker-compose.yml has no healthchecks, so "container
# started" isn't enough), applies storage/init_db.py (idempotent — every
# DDL file uses IF NOT EXISTS), then runs every app process in its own
# window of one tmux session.
#
# Usage (from anywhere):
#
#     ./scripts/dev.sh          # start everything and attach
#     ./scripts/dev.sh stop     # Ctrl+C every service, then stop the containers
#     tmux attach -t dsview     # re-attach after detaching (Ctrl+b d)
#
# tmux over a one-terminal process manager (honcho etc.) so each service's
# logs stay separate — candle_aggregator.py prints a table every 5 seconds
# and would bury everything else if interleaved.
#
# Services are typed into an interactive shell via send-keys rather than
# run as the window's command, so a crashed service leaves its window open
# with the traceback visible and the command one up-arrow away.
#
# Author: @DS

set -euo pipefail

SESSION="dsview"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_ACTIVATE="$ROOT/.venv/bin/activate"
READY_TIMEOUT_SECONDS=60
# A first-ever run has no topics yet; give ingestion a moment to publish
# (creating them) before the consumers subscribe.
INGESTION_HEADSTART_SECONDS=5
STOP_GRACE_SECONDS=5

# window name | working dir (relative to repo root) | command
SERVICES=(
  "ingestion|ingestion|python main.py"
  "whales|streaming|python -m jobs.whale_detector"
  "candles|streaming|python -m jobs.candle_aggregator"
  "storage|streaming|python -m jobs.storage_writer"
  "backend|backend|uvicorn main:app --reload --port 8000"
  "frontend|frontend|npm run dev"
)

attach() {
  if [[ -n "${TMUX:-}" ]]; then
    tmux switch-client -t "$SESSION"
  else
    tmux attach -t "$SESSION"
  fi
}

wait_until() {
  local name="$1"
  shift
  local waited=0
  printf 'Waiting for %s' "$name"
  until "$@"; do
    if ((waited >= READY_TIMEOUT_SECONDS)); then
      printf '\n%s not ready after %ss — check: docker compose logs %s\n' \
        "$name" "$READY_TIMEOUT_SECONDS" "$name" >&2
      exit 1
    fi
    sleep 1
    waited=$((waited + 1))
    printf '.'
  done
  printf ' ready\n'
}

# Output captured rather than piped into grep -q: grep exiting early can
# SIGPIPE the producer, which pipefail would report as a failure.
redpanda_ready() {
  local health
  health="$(docker exec dsview-redpanda rpk cluster health 2>/dev/null)" || return 1
  [[ "$health" =~ Healthy:[[:space:]]+true ]]
}

redis_ready() {
  [[ "$(docker exec dsview-redis redis-cli ping 2>/dev/null)" == "PONG" ]]
}

# -h 127.0.0.1, not the default unix socket: on a fresh volume the image's
# temporary init server listens on the socket only, and would pass a
# socket check before the real server is reachable on 5432.
postgres_ready() {
  docker exec dsview-postgres pg_isready -h 127.0.0.1 -U dsview -d dsview >/dev/null 2>&1
}

stop() {
  if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "Sending Ctrl+C to every service..."
    local service name
    for service in "${SERVICES[@]}"; do
      IFS='|' read -r name _ _ <<<"$service"
      tmux send-keys -t "$SESSION:$name" C-c
    done
    sleep "$STOP_GRACE_SECONDS"
    tmux kill-session -t "$SESSION"
  fi
  echo "Stopping containers (volumes kept)..."
  docker compose stop
}

start() {
  if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "tmux session '$SESSION' is already running — attaching."
    attach
    return
  fi

  local tool
  for tool in tmux docker; do
    command -v "$tool" >/dev/null || { echo "$tool not found on PATH" >&2; exit 1; }
  done
  [[ -f "$VENV_ACTIVATE" ]] || { echo "No venv at $ROOT/.venv — see CLAUDE.md's Setup" >&2; exit 1; }

  docker compose up -d
  wait_until redpanda redpanda_ready
  wait_until redis redis_ready
  wait_until postgres postgres_ready

  "$ROOT/.venv/bin/python" storage/init_db.py

  local service name dir cmd first=1
  for service in "${SERVICES[@]}"; do
    IFS='|' read -r name dir cmd <<<"$service"
    if ((first)); then
      tmux new-session -d -s "$SESSION" -n "$name" -c "$ROOT/$dir"
      first=0
    else
      tmux new-window -t "$SESSION" -n "$name" -c "$ROOT/$dir"
    fi
    tmux send-keys -t "$SESSION:$name" "source '$VENV_ACTIVATE' && $cmd" C-m
    if [[ "$name" == "ingestion" ]]; then
      sleep "$INGESTION_HEADSTART_SECONDS"
    fi
  done

  tmux select-window -t "$SESSION:ingestion"
  attach
  echo "Detached. Re-attach: tmux attach -t $SESSION — stop everything: ./scripts/dev.sh stop"
}

cd "$ROOT"

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  *)
    echo "Usage: $0 [start|stop]" >&2
    exit 1
    ;;
esac
