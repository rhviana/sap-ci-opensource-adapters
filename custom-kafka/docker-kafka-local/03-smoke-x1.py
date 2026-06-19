# EventSmartKafka — local Kafka reset/start
# Demo user: sdia / SDIABRASIL
# Ports: 19101 PLAINTEXT | 19102 SASL_PLAINTEXT | 19103 SASL_SSL

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

Write-Host "=== EventSmartKafka Local Kafka Demo ===" -ForegroundColor Cyan
Write-Host "[1/6] Resetting Docker stack..." -ForegroundColor Yellow
docker compose down --remove-orphans -v
Start-Sleep -Seconds 3

Write-Host "[2/6] Starting Kafka..." -ForegroundColor Yellow
docker compose up -d
Write-Host "Waiting 20 seconds for broker startup..." -ForegroundColor Cyan
Start-Sleep -Seconds 20

docker ps --filter "name=sdia-kafka"

Write-Host "[3/6] Creating SCRAM users: sdia / SDIABRASIL" -ForegroundColor Yellow
docker exec sdia-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19101 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=SDIABRASIL]' --entity-type users --entity-name sdia
docker exec sdia-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19101 --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=SDIABRASIL]' --entity-type users --entity-name sdia

Write-Host "[4/6] Copying client properties into container..." -ForegroundColor Yellow
docker exec sdia-kafka sh -c "mkdir -p /tmp/eventsmartkafka-client"
docker cp "$Root\config\client\plain.properties"    sdia-kafka:/tmp/eventsmartkafka-client/plain.properties
docker cp "$Root\config\client\scram256.properties" sdia-kafka:/tmp/eventsmartkafka-client/scram256.properties
docker cp "$Root\config\client\scram512.properties" sdia-kafka:/tmp/eventsmartkafka-client/scram512.properties
docker cp "$Root\config\client\sslplain.properties" sdia-kafka:/tmp/eventsmartkafka-client/sslplain.properties
docker cp "$Root\config\client\ssl256.properties"   sdia-kafka:/tmp/eventsmartkafka-client/ssl256.properties
docker cp "$Root\config\client\ssl512.properties"   sdia-kafka:/tmp/eventsmartkafka-client/ssl512.properties

Write-Host "[5/6] Validating listeners..." -ForegroundColor Yellow
docker exec sdia-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19101 | Out-Null
docker exec sdia-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19102 --command-config /tmp/eventsmartkafka-client/plain.properties | Out-Null
docker exec sdia-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19103 --command-config /tmp/eventsmartkafka-client/sslplain.properties | Out-Null

Write-Host "[6/6] Creating validation topics..." -ForegroundColor Yellow
& "$PSScriptRoot\02-create-topics.ps1"

Write-Host "" 
Write-Host "Setup complete." -ForegroundColor Green
Write-Host "Ports:" -ForegroundColor Cyan
Write-Host "  19101 = PLAINTEXT"
Write-Host "  19102 = SASL_PLAINTEXT (PLAIN, SCRAM-256, SCRAM-512)"
Write-Host "  19103 = SASL_SSL (PLAIN, SCRAM-256, SCRAM-512)"
Write-Host "" 
Write-Host "Next: python .\exec\03-smoke-x1.py" -ForegroundColor Cyan
