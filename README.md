# Event Smart Kafka Adapter for SAP Integration Suite

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


[![License: Apache 2.0 / MIT](https://img.shields.io/badge/License-Apache%202.0%20%2F%20MIT-blue.svg)](LICENSE)
[![SAP BTP](https://img.shields.io/badge/SAP%20BTP-Integration%20Suite-0FAAFF.svg)](https://www.sap.com/products/technology-platform/integration-suite.html)
[![ADK](https://img.shields.io/badge/SAP%20ADK-2.2.0-green.svg)](https://help.sap.com/docs/cloud-integration)
[![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9.2-231F20.svg)](https://kafka.apache.org)

A fully open-source, Phase 1 hardened Apache Kafka adapter for **SAP BTP Integration Suite (Cloud Integration)**. Built on the SAP ADK 2.2.0 / Camel 3.14.7 stack, targeting Java 8.

---

## What makes this adapter different

SAP does not provide native TCP access for Kafka through the ADK. The standard ADK only supports HTTP/HTTPS proxying through the SAP Cloud Connector. This adapter implements a **custom SOCKS5 TCP relay** that tunnels raw Kafka binary protocol through the SAP Connectivity service — something SAP does not ship out of the box.

Beyond the tunnel, the Phase 1 adapter implements a hardened Kafka consumer lifecycle that SAP's own adapter does not expose: explicit offset control, poison-pill handling, message recovery by timestamp, Avro/Schema Registry conversion, and multi-broker multi-tunnel support.

---

## Features

### Transport
- **Internet / Cloud brokers** — direct TLS or PLAINTEXT connection
- **On-Premise via SAP Cloud Connector** — custom SOCKS5 TCP tunnel with SAP proprietary JWT auth (method `0x80`), RFC 1929 fallback, and `NO_AUTH` disabled by default
- **Multi-broker support** — one relay per bootstrap broker port, passive reuse only for relays owned by the same CPI JVM/runtime registry

### Security — Phase 1 scope
- Authentication NONE — no broker credentials are requested, resolved, cached, or injected
- SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512
- PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
- TLS with public CA via platform/JVM trust
- TLS with private broker CA via SAP CPI Keystore certificate alias, PEM in-memory, no filesystem truststore
- OAuth/OAUTHBEARER, mTLS/client certificate and Kerberos/GSSAPI are intentionally blocked in Phase 1
- Bearer JWT is converted to `byte[]` inside the Cloud Connector relay; per-handshake copies are zeroed immediately. The original SAP-provided token `String` remains owned by the SAP Connectivity API/runtime.
- SASL credentials are resolved from SAP CPI Security Material only when Authentication = SASL

### Offset & error control
- Manual commit only — `enable.auto.commit=false` enforced
- **Stop on Error** — freeze on first failure, do not advance offset
- **Skip Failed Message** — commit poison offset + 1, continue
- **Retry Failed Message** — exponential backoff retry (5–10 attempts)
- **Silence Commit** — commit without delivering to iFlow (controlled drain)
- **Auto Offset Reset** — `earliest`, `latest`, `none`

### Message Recovery
- Seek by timestamp (ISO-8601, Unix epoch ms/s, auto-detect)
- Single-message isolation mode — process one recovered record without moving the consumer group position
- Original committed offset restored after isolated recovery

### Avro / Schema Registry
- Confluent wire format (magic byte `0x00` + 4-byte schema ID + Avro binary)
- Fixed Schema ID mode
- Schema JSON + parsed schema tree cached for **1 hour** (TTL-based, LRU eviction at 256 entries)
- Schema JSON size: 1 – 100 KB configurable (hard ceiling 100 KB)
- Avro → JSON and Avro → XML conversion without a Schema Registry client dependency
- JSON pass-through mode (payload already JSON)

### Payload limits
| Mode | Limit |
|---|---|
| Avro conversion | 1 – 5 MB (configurable) |
| Raw pass-through | 20 MB hard block |
| Schema JSON | 1 – 100 KB (configurable) |

---

## Supported Kafka versions

| Kafka | Status |
|---|---|
| Apache Kafka 3.9.x | Primary target for Java 8 CPI/ADK builds |
| Apache Kafka 4.x | Not bundled by default; validate Java/CPI compatibility before switching clients |
| Confluent Platform 7.x | Expected via Kafka protocol + Schema Registry HTTP API; validate in target tenant |
| Amazon MSK | Protocol-compatible target; validate networking/auth in tenant |
| Azure Event Hubs (Kafka surface) | Protocol-compatible target; validate SASL/TLS and topic semantics |
| Redpanda | Protocol-compatible target; validate Schema Registry behavior |

---

## Prerequisites

| Requirement | Version |
|---|---|
| SAP BTP Integration Suite | Cloud Integration (any current version) |
| SAP ADK | 2.2.0 |
| Java (compile target) | 8 |
| Maven | 3.6+ |
| Apache Kafka (local dev) | `apache/kafka:4.1.2` or any 3.x/4.x |

---

## Build

```bash
git clone https://github.com/your-org/sdia-kafka-adk.git
cd sdia-kafka-adk
mvn clean install
```

The `.esa` artifact is produced under the project root after `install`. Deploy it to SAP Integration Suite via **Design → Import**.

---

## Cloud Connector — TCP tunnel setup

This adapter implements what SAP does not provide: raw TCP tunneling of Kafka binary protocol through the SAP Cloud Connector.

### How it works

```
CPI Worker
  └── SdiaKafkaConsumer (Kafka client → localhost:<brokerPort>)
        └── SdiaCloudConnectorTcpTunnel (local TCP relay)
              └── SAP Connectivity SOCKS5 proxy (port 20004)
                    └── SAP Cloud Connector
                          └── Internal Kafka broker
```

The Kafka client connects to `127.0.0.1:<brokerPort>`. The relay opens a SOCKS5 `CONNECT` through the SAP Connectivity service using the JWT from `CloudConnectorProperties.getAdditionalHeaders()`. The relay forwards the raw Kafka binary stream — no protocol interpretation.

### Required Kafka broker configuration

The broker's client-facing advertised listener **must** resolve back through the relay:

```properties
# server.properties or docker-compose
advertised.listeners=PLAINTEXT://127.0.0.1:9092
```

### Cloud Connector mapping

In the Cloud Connector administration UI, map the virtual host to the internal broker:

| Field | Value |
|---|---|
| Virtual Host | `kafka-broker.internal` (any name) |
| Virtual Port | `9092` |
| Internal Host | `actual-broker-hostname` |
| Internal Port | `9092` |
| Protocol | `TCP` |

In the adapter channel, set **Bootstrap Servers** to `kafka-broker.internal:9092` and **Proxy Type** to `On-Premise`.

### Location ID

The **SAP-Connectivity-SCC-Location_ID** header is optional. Set it only when the Cloud Connector subaccount connection uses a Location ID. Leave blank for the default (unnamed) connection.

---

## Adapter channel parameters

| Parameter | Description | Default |
|---|---|---|
| `kafkaClusterHostsTable` | Bootstrap broker host/port table | — |
| `topicPattern` | Topic name, comma-separated list, or wildcard pattern | — |
| `groupId` | Consumer group ID (auto-generated if blank) | auto |
| `authentication` | `None`, `SASL` | `None` |
| `connectWithTls` | Enable TLS. NONE+TLS=`SSL`; SASL+TLS=`SASL_SSL`; SASL without TLS=`SASL_PLAINTEXT` | `false` |
| `saslMechanism` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`; visible only for SASL | `PLAIN` |
| `credentialAlias` | SAP CPI Security Material alias; required only for SASL and ignored for NONE | — |
| `brokerCaSource` | `None` for public/platform CA, `Custom` for private CA from SAP CPI Keystore; used only with TLS | `None` |
| `certificateAlias` | SAP CPI Keystore certificate alias for private broker CA; not mTLS | — |
| `proxyType` | `Internet` or `On-Premise` | `Internet` |
| `sapSapccLocationId` | Cloud Connector Location ID (optional) | — |
| `autoOffsetReset` | `earliest`, `latest`, `none` | `earliest` |
| `maxPollRecords` | Records per poll | `10` |
| `errorHandling` | `Stop on Error`, `Skip Failed Message`, `Retry Failed Message` | `Stop on Error` |
| `conversionFormat` | `None`, `JSON`, `XML`, `JSON_SCHEMA` | `None` |
| `schemaRegistryHostAddress` | Schema Registry URL | — |
| `schemaRegistryCredentialAlias` | Schema Registry credentials alias | — |
| `schemaIdResolutionMode` | `Magic Byte` or `Fixed Schema ID` | `Magic Byte` |
| `schemaRegistrySchemaId` | Fixed schema ID (numeric) | — |
| `maxPayloadConversionSize` | Max Avro conversion payload in MB (1–5) | `1` |
| `schemaIdBufferSize` | Max schema JSON size in KB (1–100) | `50` |
| `recoveryMode` | `Disabled` or `Seek by Timestamp` | `Disabled` |
| `recoveryTimestampValue` | Recovery timestamp value | — |
| `recoveryTimestampFormat` | `ISO-8601 UTC`, `Unix Epoch seconds`, `Unix Epoch milliseconds`, `Auto Detect` | `Auto Detect` |

---

## License

Dual-licensed under [Apache License 2.0](LICENSE-APACHE) and [MIT License](LICENSE-MIT). Use whichever suits your project.

## Author

Ricardo Luz Holanda Viana  
Independent Solo Researcher | Enterprise Integration Architecture  
SAP Press Author — *Enterprise Messaging* (SAP Press, 2021)  
Creator of DEIP · SDIA · GDCR · DDCR · ODCP · EDCP · DDCP  
ORCID: [0009-0009-9549-5862](https://orcid.org/0009-0009-9549-5862)
