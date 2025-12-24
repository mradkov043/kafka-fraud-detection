# stop-all.ps1
$ErrorActionPreference = "Stop"

$pidFile = Join-Path $PSScriptRoot ".service-pids.json"

if (!(Test-Path $pidFile)) {
  Write-Host "No PID file found at: $pidFile"
  Write-Host "Nothing to stop."
  exit 0
}

$pids = Get-Content $pidFile | ConvertFrom-Json
$parentPids = @($pids.Fraud, $pids.Alert, $pids.Producer)

function Stop-Tree($ppid) {
  if (!$ppid) { return }

  # Kill child processes first (java started by maven)
  $children = Get-CimInstance Win32_Process -Filter "ParentProcessId=$ppid" -ErrorAction SilentlyContinue
  foreach ($c in $children) {
    try { Stop-Process -Id $c.ProcessId -Force -ErrorAction SilentlyContinue } catch {}
  }

  # Kill the parent powershell window
  try { Stop-Process -Id $ppid -Force -ErrorAction SilentlyContinue } catch {}
}

Write-Host "Stopping services..."
foreach ($ppid in $parentPids) { Stop-Tree $ppid }

# Optional: if ports are still busy, kill by port (edit if your ports differ)
$ports = @(8081, 8082, 8083)
foreach ($port in $ports) {
  $conns = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue
  foreach ($c in $conns) {
    try { Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue } catch {}
  }
}

Remove-Item $pidFile -ErrorAction SilentlyContinue
Write-Host "Done."
