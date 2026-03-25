#!/usr/bin/env bash

set -euo pipefail

# Resolve repo root from script location.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SERVICES=(
  postgres
  redis
  oms
  pms
  risk
  market_data
  scheduler
)

if ! command -v docker >/dev/null 2>&1; then
  echo "Error: docker is not installed or not in PATH." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Error: docker compose plugin is not available." >&2
  echo "Install Docker Desktop (or Docker Compose v2) and try again." >&2
  exit 1
fi

if [[ ! -f "${REPO_ROOT}/docker-compose.yml" ]]; then
  echo "Error: docker-compose.yml not found at ${REPO_ROOT}." >&2
  exit 1
fi

if [[ ! -f "${REPO_ROOT}/.env" ]]; then
  echo "Warning: .env not found at ${REPO_ROOT}/.env." >&2
  echo "Compose may prompt/expand empty vars depending on your shell env." >&2
fi

echo "Starting core services (without pgadmin/redisinsight): ${SERVICES[*]}"
docker compose -f "${REPO_ROOT}/docker-compose.yml" up -d "${SERVICES[@]}"

echo "Done. Current status:"
docker compose -f "${REPO_ROOT}/docker-compose.yml" ps
