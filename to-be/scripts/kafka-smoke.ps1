# Kafka smoke: create mail.events -> produce 1 JSON -> consume 1 message
# PowerShell:  from repo root,  .\to-be\scripts\kafka-smoke.ps1
# CMD:         from repo root,  to-be\scripts\kafka-smoke.bat  (Bypass execution policy)
# Prerequisite: cd to-be && docker compose up -d

$ErrorActionPreference = "Stop"
$toBeRoot = Split-Path -Parent $PSScriptRoot
if (-not (Test-Path (Join-Path $toBeRoot "docker-compose.yml"))) {
    throw "docker-compose.yml not found (expected to-be/ next to scripts/)."
}
Set-Location $toBeRoot

$bootstrap = "kafka:29092"
$topic = "mail.events"

Write-Host "==> Working directory: $toBeRoot"

Write-Host "==> Delete topic if exists (then recreate so first consume is predictable)"
$prev = $ErrorActionPreference
$ErrorActionPreference = "SilentlyContinue"
docker compose exec -T kafka kafka-topics --bootstrap-server $bootstrap --delete --topic $topic 2>&1 | Out-Null
$ErrorActionPreference = $prev
Start-Sleep -Seconds 4

Write-Host "==> Create topic: $topic"
docker compose exec -T kafka kafka-topics --bootstrap-server $bootstrap --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1

Write-Host "==> List topics"
docker compose exec -T kafka kafka-topics --bootstrap-server $bootstrap --list

$mid = ([guid]::NewGuid().ToString("n").Substring(0, 8))
$payload = "{`"mail_id`":`"$mid`",`"action`":`"created`"}"
Write-Host "==> Produce: $payload"
$payload | docker compose exec -T kafka kafka-console-producer --bootstrap-server $bootstrap --topic $topic

Write-Host "==> Consume (first message)"
$received = docker compose exec -T kafka kafka-console-consumer --bootstrap-server $bootstrap --topic $topic --from-beginning --max-messages 1 --timeout-ms 20000

Write-Host "   Received: $($received.Trim())"
if ($received -match [regex]::Escape($mid)) {
    Write-Host "==> OK: mail_id matches"
    exit 0
}
Write-Host "==> FAIL: expected mail_id not found in payload"
exit 1
