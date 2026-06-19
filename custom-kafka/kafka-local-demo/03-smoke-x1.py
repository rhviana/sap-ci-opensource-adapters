# EventSmartKafka — create local validation topics
$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "..\..")
Set-Location $Root

function New-TopicPlain($topic) {
  docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $topic --bootstrap-server localhost:19101 --partitions 1 --replication-factor 1
}
function New-TopicSasl($topic, $config) {
  docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $topic --bootstrap-server localhost:19102 --command-config $config --partitions 1 --replication-factor 1
}
function New-TopicSsl($topic, $config) {
  docker exec sdia-kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic $topic --bootstrap-server localhost:19103 --command-config $config --partitions 1 --replication-factor 1
}

Write-Host "Creating PLAINTEXT topics on 19101..." -ForegroundColor Yellow
New-TopicPlain "id00.acme.sales.otc.orders.created.n.s"
New-TopicPlain "id13.acme.sales.otc.orders.created.n.s.cj"
New-TopicPlain "id14.acme.sales.otc.orders.created.n.s.cj.1mb"
New-TopicPlain "id15.acme.sales.otc.orders.created.n.s.cj.2mb"
New-TopicPlain "id16.acme.sales.otc.orders.created.n.s.cx"
New-TopicPlain "id17.acme.sales.otc.orders.created.n.s.cx.1mb"
New-TopicPlain "id18.acme.sales.otc.orders.created.n.s.cx.2mb"

Write-Host "Creating SASL_PLAINTEXT topics on 19102..." -ForegroundColor Yellow
New-TopicSasl "id01.acme.sales.otc.fulfillment.started.s.p"          "/tmp/eventsmartkafka-client/plain.properties"
New-TopicSasl "id03.acme.sales.otc.orders.updated.s.s.256"           "/tmp/eventsmartkafka-client/scram256.properties"
New-TopicSasl "id05.acme.sales.otc.shipments.dispatched.s.s.512"     "/tmp/eventsmartkafka-client/scram512.properties"
New-TopicSasl "id19.acme.sales.otc.orders.created.n.s.cj.53kb"       "/tmp/eventsmartkafka-client/plain.properties"
New-TopicSasl "id20.acme.sales.otc.orders.created.n.s.cj.53kb"       "/tmp/eventsmartkafka-client/plain.properties"
New-TopicSasl "id21.acme.sales.otc.orders.created.n.s.cj.1mb"        "/tmp/eventsmartkafka-client/scram256.properties"
New-TopicSasl "id22.acme.sales.otc.orders.created.n.s.cj.2mb"        "/tmp/eventsmartkafka-client/scram512.properties"

Write-Host "Creating SASL_SSL topics on 19103..." -ForegroundColor Yellow
New-TopicSsl "id02.acme.sales.otc.fulfillment.started.s.tls.p"        "/tmp/eventsmartkafka-client/sslplain.properties"
New-TopicSsl "id04.acme.sales.otc.orders.updated.s.tls.s.256"         "/tmp/eventsmartkafka-client/ssl256.properties"
New-TopicSsl "id06.acme.sales.otc.shipments.dispatched.s.tls.s.512"   "/tmp/eventsmartkafka-client/ssl512.properties"
New-TopicSsl "id23.acme.sales.otc.orders.created.n.s.cj.53kb"         "/tmp/eventsmartkafka-client/sslplain.properties"
New-TopicSsl "id24.acme.sales.otc.orders.created.n.s.cj.1mb"          "/tmp/eventsmartkafka-client/ssl256.properties"
New-TopicSsl "id25.acme.sales.otc.orders.created.n.s.cj.2mb"          "/tmp/eventsmartkafka-client/ssl512.properties"

Write-Host "Topics created or already existing." -ForegroundColor Green
