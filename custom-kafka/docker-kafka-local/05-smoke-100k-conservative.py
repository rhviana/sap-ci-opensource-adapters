# EventSmartKafka — list local topics
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:19101
