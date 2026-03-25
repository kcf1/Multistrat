#!/usr/bin/env bash
#
# Deploy workflow for Phase 5 + Market Data (core-first + backfill before market_data):
#   1) (manual) set `.env`
#   2) docker up (infra only: postgres, redis)
#   3) run Alembic migrations
#   4) seed assets (init_assets by default; --destructive-seed for reset_and_seed_assets)
#   5) docker start core apps (oms, pms, risk, scheduler) (market_data still OFF)
#   6) backfill market data with NO watermarks
#   7) start `market_data`
#
# Run from repo root:
#   ./scripts/deploy_stack.sh
#
# Flags:
#   --no-build     Skip rebuilding app images
#   --with-tools   Also start pgadmin + redisinsight
#   --skip-existing Only with backfill (attempt to skip contiguous existing history)
#   --destructive-seed  Run scripts/reset_and_seed_assets.py (truncates assets) instead of init_assets.py
#   --dry-run       Print what would run

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

NO_BUILD=0
WITH_TOOLS=0
SKIP_EXISTING=0
DESTRUCTIVE_SEED=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)
      NO_BUILD=1
      ;;
    --with-tools)
      WITH_TOOLS=1
      ;;
    --skip-existing)
      SKIP_EXISTING=1
      ;;
    --destructive-seed)
      DESTRUCTIVE_SEED=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      echo "Usage: $0 [--no-build] [--with-tools] [--skip-existing] [--destructive-seed] [--dry-run]"
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      exit 1
      ;;
  esac
  shift
done

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed or not in PATH." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Error: docker compose plugin is not available." >&2
  exit 1
fi

if [[ ! -f "${REPO_ROOT}/.env" ]]; then
  echo "Error: .env not found. Copy .env.example to .env and set POSTGRES_* (and BINANCE_* if needed)." >&2
  exit 1
fi

COMPOSE_ARGS=(-f "${REPO_ROOT}/docker-compose.yml")

run() {
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "[dry-run] $*"
    return 0
  fi
  echo "+ $*"
  "$@"
}

wait_healthy() {
  local svc="$1"
  local tries="${2:-60}" # ~2 minutes at 2s intervals

  local i cid status
  for ((i=1; i<=tries; i++)); do
    cid="$(docker compose "${COMPOSE_ARGS[@]}" ps -q "${svc}" || true)"
    if [[ -z "${cid}" ]]; then
      sleep 2
      continue
    fi
    # If no healthcheck exists, consider it "unknown" and keep waiting.
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "${cid}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "Error: timed out waiting for ${svc} to become healthy." >&2
  exit 1
}

APP_SERVICES=(oms pms risk scheduler market_data)
INFRA_SERVICES=(postgres redis)
TOOLS_SERVICES=(pgadmin redisinsight)

if [[ "${NO_BUILD}" -eq 0 ]]; then
  run docker compose "${COMPOSE_ARGS[@]}" build --pull "${APP_SERVICES[@]}"
fi

echo "Starting infra only: ${INFRA_SERVICES[*]}"
run docker compose "${COMPOSE_ARGS[@]}" up -d "${INFRA_SERVICES[@]}"

if [[ "${WITH_TOOLS}" -eq 1 ]]; then
  echo "Starting optional tools: ${TOOLS_SERVICES[*]}"
  run docker compose "${COMPOSE_ARGS[@]}" up -d "${TOOLS_SERVICES[@]}"
fi

wait_healthy postgres
wait_healthy redis

echo "Running DB migrations (alembic upgrade head)..."
run docker compose "${COMPOSE_ARGS[@]}" run --rm oms python -m alembic upgrade head

if [[ "${DESTRUCTIVE_SEED}" -eq 1 ]]; then
  echo "Seeding assets (destructive: reset_and_seed_assets)..."
  run docker compose "${COMPOSE_ARGS[@]}" run --rm oms python scripts/reset_and_seed_assets.py
else
  echo "Seeding assets (init_assets)..."
  run docker compose "${COMPOSE_ARGS[@]}" run --rm oms python scripts/init_assets.py
fi

echo "Starting core apps (market_data OFF): oms pms risk scheduler"
run docker compose "${COMPOSE_ARGS[@]}" up -d oms pms risk scheduler

BACKFILL_ARGS=(python scripts/backfill_all_no_watermarks.py)
if [[ "${SKIP_EXISTING}" -eq 1 ]]; then
  BACKFILL_ARGS+=(--skip-existing)
fi

echo "Running backfill (no watermarks) before starting market_data..."
run docker compose "${COMPOSE_ARGS[@]}" run --rm oms "${BACKFILL_ARGS[@]}"

echo "Starting market_data..."
run docker compose "${COMPOSE_ARGS[@]}" up -d market_data

echo "Done. Current status:"
run docker compose "${COMPOSE_ARGS[@]}" ps
