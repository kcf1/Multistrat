#!/usr/bin/env bash
#
# Update deploy workflow for an existing environment:
#   1) (optional) rebuild app images
#   2) ensure infra is running (postgres, redis)
#   3) run Alembic migrations
#   4) restart app services to pick up changes
#
# Run from repo root:
#   ./scripts/deploy_stack_update.sh
#
# Flags:
#   --no-build    Skip rebuilding app images
#   --with-tools  Also ensure pgadmin + redisinsight are running
#   --dry-run     Print what would run
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

NO_BUILD=0
WITH_TOOLS=0
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)
      NO_BUILD=1
      ;;
    --with-tools)
      WITH_TOOLS=1
      ;;
    --dry-run)
      DRY_RUN=1
      ;;
    -h|--help)
      echo "Usage: $0 [--no-build] [--with-tools] [--dry-run]"
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
  echo "Error: .env not found. Copy .env.example to .env and set required values." >&2
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
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' "${cid}" 2>/dev/null || true)"
    if [[ "${status}" == "healthy" ]]; then
      return 0
    fi
    sleep 2
  done

  echo "Error: timed out waiting for ${svc} to become healthy." >&2
  exit 1
}

APP_SERVICES=(oms market_data pms risk scheduler)
INFRA_SERVICES=(postgres redis)
TOOLS_SERVICES=(pgadmin redisinsight)

if [[ "${NO_BUILD}" -eq 0 ]]; then
  echo "Building app images: ${APP_SERVICES[*]}"
  run docker compose "${COMPOSE_ARGS[@]}" build --pull "${APP_SERVICES[@]}"
fi

echo "Ensuring infra services are running: ${INFRA_SERVICES[*]}"
run docker compose "${COMPOSE_ARGS[@]}" up -d "${INFRA_SERVICES[@]}"

if [[ "${WITH_TOOLS}" -eq 1 ]]; then
  echo "Ensuring optional tools are running: ${TOOLS_SERVICES[*]}"
  run docker compose "${COMPOSE_ARGS[@]}" up -d "${TOOLS_SERVICES[@]}"
fi

wait_healthy postgres
wait_healthy redis

echo "Running DB migrations (alembic upgrade head)..."
run docker compose "${COMPOSE_ARGS[@]}" run --rm oms python -m alembic upgrade head

echo "Restarting app services with latest image/code..."
run docker compose "${COMPOSE_ARGS[@]}" up -d --force-recreate "${APP_SERVICES[@]}"

echo "Done. Current status:"
run docker compose "${COMPOSE_ARGS[@]}" ps
