#!/usr/bin/env bash
#
# Legacy full local bootstrap: build app images, Alembic upgrade, asset seed, docker compose up ALL services.
# Run from repository root after copying .env.example -> .env and editing secrets.
#
# Usage:
#   ./scripts/deploy_stack_legacy_all_services.sh
#   ./scripts/deploy_stack_legacy_all_services.sh --no-build
#   ./scripts/deploy_stack_legacy_all_services.sh --destructive-seed
#   ./scripts/deploy_stack_legacy_all_services.sh --with-tools
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

NO_BUILD=0
DESTRUCTIVE_SEED=0
WITH_TOOLS=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)
      NO_BUILD=1
      ;;
    --destructive-seed)
      DESTRUCTIVE_SEED=1
      ;;
    --with-tools)
      WITH_TOOLS=1
      ;;
    -h|--help)
      echo "Usage: $0 [--no-build] [--destructive-seed] [--with-tools]"
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
  echo "Error: .env not found. Copy .env.example to .env and set POSTGRES_* and other values." >&2
  exit 1
fi

APP_SERVICES=(oms pms risk market_data scheduler)
INFRA_SERVICES=(postgres redis)
UP_SERVICES=("${INFRA_SERVICES[@]}" "${APP_SERVICES[@]}")

if [[ "${WITH_TOOLS}" -eq 1 ]]; then
  UP_SERVICES+=(pgadmin redisinsight)
fi

if [[ "${NO_BUILD}" -eq 0 ]]; then
  echo "Building app images (${APP_SERVICES[*]})..."
  docker compose build --pull "${APP_SERVICES[@]}"
fi

echo "Running database migrations (alembic upgrade heads)..."
docker compose run --rm oms python -m alembic upgrade heads

if [[ "${DESTRUCTIVE_SEED}" -eq 1 ]]; then
  echo "Seeding assets (destructive: reset_and_seed_assets)..."
  docker compose run --rm oms python scripts/reset_and_seed_assets.py
else
  echo "Seeding assets (init_assets)..."
  docker compose run --rm oms python scripts/init_assets.py
fi

echo "Starting services: ${UP_SERVICES[*]}"
docker compose up -d "${UP_SERVICES[@]}"

echo "Done."
docker compose ps

