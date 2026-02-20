# Update and deploy only coded app services (oms, pms). Skips postgres, redis, pgadmin, redisinsight.
# Ensures DB is migrated (alembic upgrade head) then builds and starts oms and pms.
# Run from repo root (where docker-compose.yml is).
# Usage: .\scripts\update_and_deploy.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

Write-Host "Running DB migrations (alembic upgrade head)..."
docker compose run --rm oms python -m alembic upgrade head

Write-Host "Rebuilding app image (oms + pms)..."
docker compose build --pull oms

Write-Host "Deploying oms and pms (force recreate so new image is used)..."
docker compose up -d --force-recreate oms pms

Write-Host "Done."
docker compose ps
