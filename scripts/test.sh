#!/usr/bin/env bash
#
# test.sh
#
# Runs DSView's whole test suite with one command: starts throwaway Kafka
# (Redpanda), Redis and Postgres in Docker (tests/docker-compose.yml, its
# own "dsview-test" compose project), waits until they answer, runs the
# pytest suite in tests/ against them, then the frontend's Vitest tests
# (frontend/src/**/*.test.js, which need no services). Both always run;
# the exit status is non-zero if either fails.
#
# Usage (from anywhere):
#
#     ./scripts/test.sh                  # test services in Docker, then the whole suite
#     ./scripts/test.sh -k whale         # any other arguments go to pytest
#     ./scripts/test.sh --no-frontend    # Python tests only
#     ./scripts/test.sh -m "not spark"   # e.g. skip the slower Spark tests
#     ./scripts/test.sh --no-docker      # use services already running (CI)
#     ./scripts/test.sh down             # remove the test services
#
# The test services are never the dev stack's: the suite publishes
# synthetic whales, which on the dev broker would reach the right panel and
# the Telegram bot. They use their own ports, so `./scripts/dev.sh` can keep
# running alongside, and stay up between runs so a rerun starts quickly.
#
# With --no-docker, DSVIEW_KAFKA_BOOTSTRAP_SERVERS, DSVIEW_REDIS_PORT and
# DSVIEW_POSTGRES_DSN (plus DSVIEW_REDIS_HOST if not localhost) must say
# where the services are. Every other DSVIEW_* variable is cleared either
# way, so a threshold or bot token set in your shell can't change what the
# tests see or where they send.
#
# Author: @DS

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/tests/docker-compose.yml" -p dsview-test)
SERVICE_VARS=(DSVIEW_KAFKA_BOOTSTRAP_SERVERS DSVIEW_REDIS_HOST DSVIEW_REDIS_PORT DSVIEW_POSTGRES_DSN)

if [[ "${1:-}" == "down" ]]; then
  "${COMPOSE[@]}" down
  exit 0
fi

use_docker=1
run_frontend=1
while [[ "${1:-}" == --no-docker || "${1:-}" == --no-frontend ]]; do
  case "$1" in
    --no-docker) use_docker=0 ;;
    --no-frontend) run_frontend=0 ;;
  esac
  shift
done

# Keep only where the services are; drop every other DSVIEW_* override.
declare -A keep=()
for name in "${SERVICE_VARS[@]}"; do
  if [[ -n "${!name:-}" ]]; then
    keep[$name]="${!name}"
  fi
done
while read -r name; do
  unset "$name"
done < <(compgen -e | grep '^DSVIEW_' || true)

if ((use_docker)); then
  command -v docker >/dev/null || { echo "docker not found on PATH (use --no-docker if the services run elsewhere)" >&2; exit 1; }
  "${COMPOSE[@]}" up -d
  export DSVIEW_KAFKA_BOOTSTRAP_SERVERS="localhost:19092"
  export DSVIEW_REDIS_HOST="localhost"
  export DSVIEW_REDIS_PORT="16379"
  export DSVIEW_POSTGRES_DSN="postgresql://dsview:dsview@localhost:15432/dsview_test"
else
  for name in "${!keep[@]}"; do
    export "$name=${keep[$name]}"
  done
  for name in DSVIEW_KAFKA_BOOTSTRAP_SERVERS DSVIEW_REDIS_PORT DSVIEW_POSTGRES_DSN; do
    [[ -n "${!name:-}" ]] || { echo "--no-docker needs $name set to the test service" >&2; exit 1; }
  done
fi

# The venv dev.sh uses, unless one is already active; plain python3 in CI.
if [[ -n "${VIRTUAL_ENV:-}" ]]; then
  PYTHON=python
elif [[ -x "$ROOT/.venv/bin/python" ]]; then
  PYTHON="$ROOT/.venv/bin/python"
else
  PYTHON=python3
fi

# For `config.settings` in Spark's Python worker processes, which inherit
# PYTHONPATH but not pytest's sys.path (see config/settings.py).
export PYTHONPATH="$ROOT"
cd "$ROOT"

"$PYTHON" tests/wait_for_services.py

status=0
"$PYTHON" -m pytest "$@" || status=$?

if ((run_frontend)); then
  if [[ ! -d frontend/node_modules ]]; then
    echo "frontend/node_modules missing: run \`cd frontend && npm install\` (CI: npm ci)" >&2
    status=1
  else
    (cd frontend && npm test) || status=$?
  fi
fi

exit "$status"
