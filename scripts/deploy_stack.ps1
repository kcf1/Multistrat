# Deploy workflow for Phase 5 + Market Data:
#   1) (manual) set `.env`
#   2) docker up (infra only: postgres, redis)
#   3) run Alembic migrations
#   4) docker start `oms` only (symbol sync; backfill uses oms image)
#   5) backfill market data with NO watermarks
#   6) start `market_data`
#   7) docker start `pms`, `risk`, `scheduler` (PMS seeds assets at startup)
#
# Usage (from repo root):
#   .\scripts\deploy_stack.ps1
#
# Flags:
#   -NoBuild        Skip rebuilding app images
#   -WithTools      Also start pgadmin + redisinsight
#   -SkipExisting   Only with backfill (attempt to skip contiguous existing history)
#   -DryRun         Print what would run

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

param(
  [switch]$NoBuild,
  [switch]$WithTools,
  [switch]$SkipExisting,
  [switch]$DryRun
)

function RunCmd([string[]]$Cmd) {
  if ($DryRun) {
    Write-Host ("[dry-run] " + ($Cmd -join " "))
    return
  }
  Write-Host ("+ " + ($Cmd -join " "))
  & $Cmd
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  throw "Error: docker is not installed or not in PATH."
}

if (-not (docker compose version 2>$null)) {
  throw "Error: docker compose plugin is not available."
}

if (-not (Test-Path ".env")) {
  throw "Error: .env not found. Copy .env.example to .env and set POSTGRES_* (and BINANCE_* if needed)."
}

$composeFile = "docker-compose.yml"
$composeArgs = @("-f", $composeFile)

$APP_SERVICES = @("oms", "pms", "risk", "scheduler", "market_data")
$INFRA_SERVICES = @("postgres", "redis")
$TOOLS_SERVICES = @("pgadmin", "redisinsight")

function Wait-Healthy([string]$Service, [int]$Tries = 60) {
  for ($i = 1; $i -le $Tries; $i++) {
    $cid = (& docker compose @composeArgs ps -q $Service 2>$null | Out-String).Trim()
    if (-not $cid) {
      Start-Sleep -Seconds 2
      continue
    }

    $status = (& docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}unknown{{end}}' $cid 2>$null | Out-String).Trim()
    if ($status -eq "healthy") {
      return
    }
    Start-Sleep -Seconds 2
  }
  throw "Error: timed out waiting for $Service to become healthy."
}

if (-not $NoBuild) {
  $cmd = @("docker", "compose") + $composeArgs + @("build", "--pull") + $APP_SERVICES
  RunCmd $cmd
}

Write-Host "Starting infra only: $($INFRA_SERVICES -join ' ')"
{
  $cmd = @("docker", "compose") + $composeArgs + @("up", "-d") + $INFRA_SERVICES
  RunCmd $cmd
}

if ($WithTools) {
  Write-Host "Starting optional tools: $($TOOLS_SERVICES -join ' ')"
  $cmd = @("docker", "compose") + $composeArgs + @("up", "-d") + $TOOLS_SERVICES
  RunCmd $cmd
}

Wait-Healthy "postgres"
Wait-Healthy "redis"

Write-Host "Running DB migrations (alembic upgrade head)..."
{
  $cmd = @("docker", "compose") + $composeArgs + @("run", "--rm", "oms", "python", "-m", "alembic", "upgrade", "head")
  RunCmd $cmd
}

Write-Host "Starting OMS only (symbol sync before backfill; pms/risk/scheduler after market_data)..."
{
  $cmd = @("docker", "compose") + $composeArgs + @("up", "-d", "oms")
  RunCmd $cmd
}

$backfillCmd = @("docker", "compose") + $composeArgs + @("run", "--rm", "oms", "python", "scripts/backfill_all_no_watermarks.py")
if ($SkipExisting) {
  $backfillCmd += "--skip-existing"
}

Write-Host "Running backfill (no watermarks) before starting market_data..."
RunCmd $backfillCmd

Write-Host "Starting market_data..."
{
  $cmd = @("docker", "compose") + $composeArgs + @("up", "-d", "market_data")
  RunCmd $cmd
}

Write-Host "Starting pms, risk, scheduler..."
{
  $cmd = @("docker", "compose") + $composeArgs + @("up", "-d") + @("pms", "risk", "scheduler")
  RunCmd $cmd
}

Write-Host "Done. Current status:"
{
  $cmd = @("docker", "compose") + $composeArgs + @("ps")
  RunCmd $cmd
}

