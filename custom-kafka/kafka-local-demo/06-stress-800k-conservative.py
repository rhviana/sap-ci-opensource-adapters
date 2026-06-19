#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

new_topic_plain() {
  docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$1" --bootstrap-server localhost:19101 --partitions 1 --replication-factor 1
}
new_topic_sasl() {
  docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$1" --bootstrap-server localhost:19102 --command-config "$2" --partitions 1 --replication-factor 1
}
new_topic_ssl() {
  docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic "$1" --bootstrap-server localhost:19103 --command-config "$2" --partitions 1 --replication-factor 1
}

echo "Creating PLAINTEXT topics on 19101..."
new_topic_plain "id00.acme.sales.otc.orders.created.n.s"
new_topic_plain "id13.acme.sales.otc.orders.created.n.s.cj"
new_topic_plain "id14.acme.sales.otc.orders.created.n.s.cj.1mb"
new_topic_plain "id15.acme.sales.otc.orders.created.n.s.cj.2mb"
new_topic_plain "id16.acme.sales.otc.orders.created.n.s.cx"
new_topic_plain "id17.acme.sales.otc.orders.created.n.s.cx.1mb"
new_topic_plain "id18.acme.sales.otc.orders.created.n.s.cx.2mb"

echo "Creating SASL_PLAINTEXT topics on 19102..."
new_topic_sasl "id01.acme.sales.otc.fulfillment.started.s.p" "/tmp/eventsmartkafka-client/plain.properties"
new_topic_sasl "id03.acme.sales.otc.orders.updated.s.s.256" "/tmp/eventsmartkafka-client/scram256.properties"
new_topic_sasl "id05.acme.sales.otc.shipments.dispatched.s.s.512" "/tmp/eventsmartkafka-client/scram512.properties"
new_topic_sasl "id19.acme.sales.otc.orders.created.n.s.cj.53kb" "/tmp/eventsmartkafka-client/plain.properties"
new_topic_sasl "id20.acme.sales.otc.orders.created.n.s.cj.53kb" "/tmp/eventsmartkafka-client/plain.properties"
new_topic_sasl "id21.acme.sales.otc.orders.created.n.s.cj.1mb" "/tmp/eventsmartkafka-client/scram256.properties"
new_topic_sasl "id22.acme.sales.otc.orders.created.n.s.cj.2mb" "/tmp/eventsmartkafka-client/scram512.properties"

echo "Creating SASL_SSL topics on 19103..."
new_topic_ssl "id02.acme.sales.otc.fulfillment.started.s.tls.p" "/tmp/eventsmartkafka-client/sslplain.properties"
new_topic_ssl "id04.acme.sales.otc.orders.updated.s.tls.s.256" "/tmp/eventsmartkafka-client/ssl256.properties"
new_topic_ssl "id06.acme.sales.otc.shipments.dispatched.s.tls.s.512" "/tmp/eventsmartkafka-client/ssl512.properties"
new_topic_ssl "id23.acme.sales.otc.orders.created.n.s.cj.53kb" "/tmp/eventsmartkafka-client/sslplain.properties"
new_topic_ssl "id24.acme.sales.otc.orders.created.n.s.cj.1mb" "/tmp/eventsmartkafka-client/ssl256.properties"
new_topic_ssl "id25.acme.sales.otc.orders.created.n.s.cj.2mb" "/tmp/eventsmartkafka-client/ssl512.properties"

echo "Topics created or already existing."
