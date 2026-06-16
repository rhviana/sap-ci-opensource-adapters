# EventSmartKafka — SAP CPI Custom Adapter

**Consumer-only Kafka adapter for SAP Cloud Integration (CPI) built on Apache Camel 3.14 / OSGi / SAP ADK 2.27.**

Designed for enterprise event-driven integration. Supports on-premise Kafka brokers via SAP Cloud Connector TCP/SOCKS5 tunnel and cloud-hosted Kafka (Confluent, Aiven, Redpanda, IBM Event Streams) via direct TLS.

---

## Features

- **3 security profiles** — NONE/PLAINTEXT, SASL (PLAIN · SCRAM-SHA-256 · SCRAM-SHA-512), SASL+TLS
- **SAP Cloud Connector** — native TCP/SOCKS5 tunnel with SAP JWT auth (method 0x80) and RFC1929 fallback
- **Avro conversion** — zero-dependency decoder; Magic Byte (Confluent wire format) and Fixed Schema ID modes
- **Schema Registry cache** — 1-hour TTL, up to 512 schemas, auto-evict on expiry
- **Offset control** — Earliest · Latest · None · Seek by Timestamp · Message Recovery
- **Error handling** — Skip · Stop on Error · Retry (5 or 10 attempts)
- **Poison Pill** — configurable skip with silent commit
- **Payload limits** — 1 MB or 2 MB MAX for Avro conversion; schema size 5–15 KB MAX
- **Consumer-only** — Producer not included in this release

---

## Security Matrix

| Authentication | TLS | Kafka Protocol | Mechanism |
|---|---|---|---|
| NONE | off | PLAINTEXT | — |
| SASL | off | SASL_PLAINTEXT | PLAIN · SCRAM-256 · SCRAM-512 |
| SASL | on | SASL_SSL | PLAIN · SCRAM-256 · SCRAM-512 |

---

## Build

Requires Java 8+, Maven 3.6+. All dependencies resolve from Maven Central — no local JARs needed.

```bash
mvn clean install
```

The SAP ADK build plugin generates the `.aar` adapter archive in `target/` and copies the bundle JAR to `component/`.

### Key dependency versions

| Artifact | Version |
|---|---|
| `com.sap.cloud.adk:generic.api` | 2.27.0 |
| `com.sap.cloud.adk:adapter.api` | 2.27.0 |
| `org.apache.camel:camel-core` | 3.14.7 |
| `org.apache.kafka:kafka-clients` | 3.6.2 |

---

## Adapter Configuration

### Connection Tab

| Field | Description |
|---|---|
| Kafka Cluster Bootstrap Hosts | Table of `host:port` rows. Port is String — supports CPI external parameters. |
| Proxy Type | `Internet` for cloud Kafka · `On-Premise` for SAP Cloud Connector routing |
| Location ID | CC Location ID — leave empty when CC has no Location ID |
| Authentication | `None` · `SASL` |
| Connect with TLS | Enables SASL_SSL when Authentication = SASL |
| SASL Mechanism | `PLAIN` · `SCRAM-SHA-256` · `SCRAM-SHA-512` |
| Credential Alias | CPI Security Material alias (User Credentials) — required for SASL |
| Broker CA Source | `JVM Trust Store` (default) · `Custom` (alias with CA cert) |

### Processing Tab

| Field | Description |
|---|---|
| Topic | Exact topic name, comma-separated list, or wildcard pattern |
| Group ID | Consumer group ID. Leave empty for auto-generated stable ID. |
| Auto Offset Reset | `earliest` · `latest` · `none` |
| Error Handling | `Skip` · `Stop on Error` · `Retry Failed Message` |
| Retry Attempts | `5` (default) · `10` (maximum) |
| Max Payload Conversion Size | `1 MB` · `2 MB MAX` |
| Schema ID Buffer Size | `5 KB` · `10 KB` · `15 KB MAX` |

---

## SAP Cloud Connector Setup

1. In SAP CC → Cloud To On-Premise → Access Control, add a TCP mapping:

   | Field | Value |
   |---|---|
   | Protocol | TCP |
   | Internal Host | `localhost` (or broker hostname) |
   | Internal Port | broker port (e.g. `19091`) |
   | Virtual Host | `sdia-kafka` |
   | Virtual Port | `19091` |

2. In the adapter, set:
   - Proxy Type = `On-Premise`
   - Bootstrap Host = `sdia-kafka`, Port = `19091`

The adapter starts a local TCP relay (`127.0.0.1:port`) that tunnels Kafka traffic through the SAP Connectivity SOCKS5 proxy to the CC virtual host. Kafka `advertised.listeners` for the external listener **must** advertise `localhost:<port>` so that broker metadata reconnects route back through the relay.

---

## Avro Conversion

Two modes:

**Magic Byte** — payload starts with `0x00` + 4-byte schema ID (Confluent wire format). Schema ID is extracted automatically from bytes 1–4.

**Fixed Schema ID** — raw Avro binary without a Confluent header. Schema ID is configured statically on the channel.

Schema JSON is fetched from the Schema Registry on first use and cached for 1 hour. Cache holds up to 512 entries.

---

## License

Dual-licensed: Apache License 2.0 / MIT License — your choice.

Copyright © 2026 Ricardo Luz Holanda Viana — ORCID: 0009-0009-9549-5862
