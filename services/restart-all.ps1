# restart-all.ps1
# Start all Spring Boot services and store their PIDs

$ErrorActionPreference = "Stop"

# ---- PATHS ----
$producerDir = "D:\Uni\Bachelor's Thesis\kafka-fraud-detection\services\transaction-producer"
$alertDir    = "D:\Uni\Bachelor's Thesis\kafka-fraud-detection\services\alert-consumer-service"
$fraudDir    = "D:\Uni\Bachelor's Thesis\kafka-fraud-detection\services\fraud-detection-service"

$pidFile = ".service-pids.json"

# ---- STOP PREVIOUS RUN (if exists) ----
if (Test-Path $pidFile) {
    try {
        $old = Get-Content $pidFile | ConvertFrom-Json
        foreach ($id in @($old.Producer, $old.Alert, $old.Fraud)) {
            if ($id -and (Get-Process -Id $id -ErrorAction SilentlyContinue)) {
                Stop-Process -Id $id -Force
            }
        }
    } catch {}
    Remove-Item $pidFile -ErrorAction SilentlyContinue
}

Write-Host "Starting services..."

# ---- START FRAUD SERVICE ----
$fraud = Start-Process powershell `
    -PassThru `
    -WorkingDirectory $fraudDir `
    -ArgumentList "-NoExit", "mvn spring-boot:run"

Start-Sleep -Seconds 3

# ---- START ALERT SERVICE ----
$alert = Start-Process powershell `
    -PassThru `
    -WorkingDirectory $alertDir `
    -ArgumentList "-NoExit", "mvn spring-boot:run"

Start-Sleep -Seconds 3

# ---- START PRODUCER ----
$producer = Start-Process powershell `
    -PassThru `
    -WorkingDirectory $producerDir `
    -ArgumentList "-NoExit", "mvn spring-boot:run"

@{
    Fraud    = $fraud.Id
    Alert    = $alert.Id
    Producer = $producer.Id
    Started  = (Get-Date).ToString("s")
} | ConvertTo-Json | Set-Content $pidFile

Write-Host "All services started."
Write-Host "PIDs saved to $pidFile"
