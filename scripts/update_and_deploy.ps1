# Update and deploy only coded app services (oms). Skips postgres, redis, pgadmin, redisinsight.
# Run from repo root (where docker-compose.yml is).
# Usage: .\scripts\update_and_deploy.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

Write-Host "Rebuilding app image (oms)..."
docker compose build --pull oms

Write-Host "Deploying oms (deps start if needed, not updated)..."
docker compose up -d oms

Write-Host "Done."
docker compose ps
