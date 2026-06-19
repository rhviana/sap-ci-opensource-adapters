# EventSmartKafka — list local consumer groups
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

docker exec sdia-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19101 --list
