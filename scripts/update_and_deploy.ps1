# Update and deploy only coded app services (oms, pms, risk, market_data). Skips postgres, redis, pgadmin, redisinsight.
# Ensures DB is migrated (alembic upgrade head) then builds and starts oms, pms, risk, and market_data.
# Run from repo root (where docker-compose.yml is).
# Usage: .\scripts\update_and_deploy.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

Write-Host "Running DB migrations (alembic upgrade head)..."
docker compose run --rm oms python -m alembic upgrade head

# One image is built (same Dockerfile + context); oms, pms, risk, market_data all use it. Build once.
Write-Host "Rebuilding app image (single image used by oms, pms, risk, market_data)..."
docker compose build --pull oms

Write-Host "Starting oms, pms, risk, and market_data (same image; force recreate)..."
docker compose up -d --force-recreate oms pms risk market_data

Write-Host "Done."
docker compose ps
