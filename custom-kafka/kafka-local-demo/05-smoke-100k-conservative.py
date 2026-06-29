#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

echo "=== EventSmartKafka Local Kafka Demo ==="
echo "[1/6] Resetting Docker stack..."
docker compose down --remove-orphans -v || true
sleep 3

echo "[2/6] Starting Kafka..."
docker compose up -d
echo "Waiting 20 seconds for broker startup..."
sleep 20

docker ps --filter "name=sdia-kafka"

echo "[3/6] Creating SCRAM users: sdia / SDIABRASIL"
docker exec sdia-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19101 --alter --add-config 'SCRAM-SHA-256=[iterations=8192,password=SDIABRASIL]' --entity-type users --entity-name sdia
docker exec sdia-kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19101 --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=SDIABRASIL]' --entity-type users --entity-name sdia

echo "[4/6] Copying client properties into container..."
docker exec sdia-kafka sh -c "mkdir -p /tmp/eventsmartkafka-client"
docker cp "$ROOT/config/client-properties/plain.properties" sdia-kafka:/tmp/eventsmartkafka-client/plain.properties
docker cp "$ROOT/config/client-properties/scram256.properties" sdia-kafka:/tmp/eventsmartkafka-client/scram256.properties
docker cp "$ROOT/config/client-properties/scram512.properties" sdia-kafka:/tmp/eventsmartkafka-client/scram512.properties
docker cp "$ROOT/config/client-properties/sslplain.properties" sdia-kafka:/tmp/eventsmartkafka-client/sslplain.properties
docker cp "$ROOT/config/client-properties/ssl256.properties" sdia-kafka:/tmp/eventsmartkafka-client/ssl256.properties
docker cp "$ROOT/config/client-properties/ssl512.properties" sdia-kafka:/tmp/eventsmartkafka-client/ssl512.properties

echo "[5/6] Validating listeners..."
docker exec sdia-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19101 >/dev/null
docker exec sdia-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19102 --command-config /tmp/eventsmartkafka-client/plain.properties >/dev/null
docker exec sdia-kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19103 --command-config /tmp/eventsmartkafka-client/sslplain.properties >/dev/null

echo "[6/6] Creating validation topics..."
"$ROOT/setup/bash/02-create-topics.sh"

echo ""
echo "Setup complete."
echo "Ports:"
echo "  19101 = PLAINTEXT"
echo "  19102 = SASL_PLAINTEXT (PLAIN, SCRAM-256, SCRAM-512)"
echo "  19103 = SASL_SSL (PLAIN, SCRAM-256, SCRAM-512)"
echo ""
echo "Next: python tests/smoke/03-smoke-x1.py"
