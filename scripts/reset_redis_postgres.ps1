# Reset Redis (FLUSHALL) and Postgres (truncate all tables).
# Run from repo root (where docker-compose.yml is).
# Usage: .\scripts\reset_redis_postgres.ps1

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

docker compose exec redis redis-cli FLUSHALL
docker compose exec postgres psql -U multistrat -d multistrat -c "TRUNCATE TABLE orders, accounts, balances, balance_changes RESTART IDENTITY CASCADE;"
Write-Host "Done."
