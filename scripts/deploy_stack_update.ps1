# Update deploy workflow for an existing environment:
#   1) (optional) rebuild app images
#   2) ensure infra is running (postgres, redis)
#   3) run Alembic migrations
#   4) restart app services to pick up changes
#
# Run from repo root:
#   .\scripts\deploy_stack_update.ps1
#
# Flags:
#   -NoBuild    Skip rebuilding app images
#   -WithTools  Also ensure pgadmin + redisinsight are running
#   -DryRun     Print what would run

param(
  [switch]$NoBuild,
  [switch]$WithTools,
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Split-Path -Parent $ScriptDir
Set-Location $RepoRoot

function Run-Cmd([string[]]$Cmd) {
  if ($DryRun) {
    Write-Host ("[dry-run] " + ($Cmd -join " "))
    return
  }

  Write-Host ("+ " + ($Cmd -join " "))
  if ($Cmd.Count -eq 1) {
    & $Cmd[0]
  } else {
    & $Cmd[0] $Cmd[1..($Cmd.Count - 1)]
  }
}

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
  throw "Error: docker is not installed or not in PATH."
}

& docker compose version *> $null
if ($LASTEXITCODE -ne 0) {
  throw "Error: docker compose plugin is not available."
}

if (-not (Test-Path (Join-Path $RepoRoot ".env"))) {
  throw "Error: .env not found. Copy .env.example to .env and set required values."
}

$ComposeArgs = @("-f", (Join-Path $RepoRoot "docker-compose.yml"))

function Wait-Healthy([string]$Service, [int]$Tries = 60) {
  for ($i = 1; $i -le $Tries; $i++) {
    $cid = (& docker compose @ComposeArgs ps -q $Service 2>$null | Out-String).Trim()
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

$AppServices = @("oms", "market_data", "pms", "risk", "scheduler")
$InfraServices = @("postgres", "redis")
$ToolsServices = @("pgadmin", "redisinsight")

if (-not $NoBuild) {
  Write-Host "Building app images: $($AppServices -join ' ')"
  $cmd = @("docker", "compose") + $ComposeArgs + @("build", "--pull") + $AppServices
  Run-Cmd $cmd
}

Write-Host "Ensuring infra services are running: $($InfraServices -join ' ')"
$cmd = @("docker", "compose") + $ComposeArgs + @("up", "-d") + $InfraServices
Run-Cmd $cmd

if ($WithTools) {
  Write-Host "Ensuring optional tools are running: $($ToolsServices -join ' ')"
  $cmd = @("docker", "compose") + $ComposeArgs + @("up", "-d") + $ToolsServices
  Run-Cmd $cmd
}

Wait-Healthy "postgres"
Wait-Healthy "redis"

Write-Host "Running DB migrations (alembic upgrade heads)..."
$cmd = @("docker", "compose") + $ComposeArgs + @("run", "--rm", "oms", "python", "-m", "alembic", "upgrade", "heads")
Run-Cmd $cmd

Write-Host "Restarting app services with latest image/code..."
$cmd = @("docker", "compose") + $ComposeArgs + @("up", "-d", "--force-recreate") + $AppServices
Run-Cmd $cmd

Write-Host "Done. Current status:"
$cmd = @("docker", "compose") + $ComposeArgs + @("ps")
Run-Cmd $cmd
