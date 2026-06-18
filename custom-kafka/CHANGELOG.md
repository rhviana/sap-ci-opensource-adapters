# Changelog — EventSmartKafka

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-v1.1.0%20Consumer%20Final-green.svg)](#)
[![SAP CPI](https://img.shields.io/badge/SAP%20CPI-Custom%20Adapter-0A6ED1.svg)](#)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Consumer-black.svg)](#)
[![SAP Cloud Connector](https://img.shields.io/badge/SAP%20Cloud%20Connector-TCP%2FSOCKS5-1f77b4.svg)](#)
[![Java](https://img.shields.io/badge/Java-8%20bytecode-red.svg)](#)

All notable changes to **EventSmartKafka** are documented here.

---

## v1.1.0 — Consumer Final Release

> **This is the final release for the Consumer/Sender side.**
> No further updates, bug fixes, enhancements or support will be provided for this module.
> This project is open source and provided AS IS under Apache License 2.0.
> Next: Producer/Receiver adapter.

### Stress Validation

3,000,000 messages delivered across 23 topics and 4 listeners on SAP BTP Trial.
4 simultaneous tunnels — Cloud Connector SOCKS5 + OSGi Relay — zero failures in 27 minutes.
Avro payloads: 53 B · 1 MB · 2 MB.

### Added

- **Parallel Consumers** — N consumer instances sharing same `group.id`. Kafka distributes partitions automatically via group rebalance.
- **Parallel + Partition conflict detection** — fail-fast with clear error when `parallelConsumers > 1` combined with explicit partition. Prevents misconfiguration silently ignored by previous versions.
- **New group + LATEST warning** — when consumer group has no committed offsets and `auto.offset.reset=LATEST`, adapter logs a clear warning per empty poll cycle so operator knows to switch to EARLIEST.
- **Zombie state log** — when `stoppedByErrorPolicy=true`, adapter logs every 60s that it is stopped and requires undeploy/redeploy to resume. No more silent dead adapters.
- **TIMESEEK read-all-from-timestamp** — fixed via `ConsumerRebalanceListener.onPartitionsAssigned`. Consumer stays in subscribe() group; commits work normally. Previous implementation used unsubscribe/assign which caused group rebalance and commit failures.
- **TIMESEEK single-record restore fix** — skips `commitSync` restore when in `assign` mode. Previous implementation tried to restore original offset after group had already rebalanced, causing `group has already rebalanced` error on every recovery.
- **Timestamp in all record errors** — `recordContext()` now includes `Timestamp`, `EpochMs`, `Proxy` and `Group`. Error messages include TIMESEEK hint with the exact EpochMs needed to recover the failed record.
- **`max.poll.records` default changed** from 500 to 50. Cap remains 500.

### Fixed

- **OSGi Relay port lock on redeploy** — relay lifecycle now bound to OSGi bundle, not classloader. Clean redeploy every time, no restart required.
- **Infinite retry loops** — fatal startup errors (invalid broker, wrong credentials, missing keystore, wrong topic, SOCKS5 failure) now apply strict no-retry policy. Stops, logs root cause, waits for redeploy.
- **SOCKS5 tunnel bug** — `fatalInitFailure` not set on tunnel failure caused silent infinite loop. Fixed.
- **Wrong defaults** — `autoOffsetReset` earliest→latest, `errorHandling` Stop→Skip, `maxPollRecords` 10→500→50.
- **UI metadata conditional fields** — `brokerCaSource` only for SASL+TLS, `certificateAlias` only for SASL+TLS+Custom, `connectWithTls` only for SASL. mTLS and OAuth show only their specific fields.
- **mTLS ignores stale `connectWithTls=false`** — mTLS always resolves as SSL regardless of hidden persisted values.
- **NONE+TLS now maps to SSL** — previously forced PLAINTEXT. `Authentication=NONE + Connect with TLS=true` now correctly uses `security.protocol=SSL`.

### Removed

- OAuth2 / OAUTHBEARER — removed from UI and code. No cloud provider offers OAuth2 Kafka at free tier. Implementation reserved for future paid-plan scenarios.

---

## v1.0.0-RC1 — Public Technical Preview

### Status

Public demo release. SAP Community study case. Apache License 2.0. Provided AS IS. No SAP certification. No production SLA.

### Added

- Kafka Consumer Sender Adapter for SAP Cloud Integration.
- SAP ADK-based custom adapter packaging.
- Cloud Kafka support.
- On-Premise Kafka through SAP Cloud Connector TCP/SOCKS5.
- Security profiles: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
- SASL mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
- TLS trust source: JVM default truststore, CPI keystore alias.
- Avro conversion: Magic Byte / Confluent Wire Format, Fixed Schema ID, Avro to JSON, Avro to XML.
- Schema Registry lookup and cache.
- Offset reset policies: EARLIEST, LATEST, NONE.
- Error handling: Stop on Error, Skip Failed Message, Retry Failed Message.
- TIMESEEK recovery by timestamp.
- Single Message Recovery mode.
- Runtime diagnostic headers (`x-sdiakafka-*`).
- Structured error messages with code, context, root cause and fix steps.
- Offset Commit Timeout configurable parameter.

### Validation

- 1.5M+ messages across stress scenarios.
- Multiple CPI iFlows, multiple topics, multiple security profiles.
- Three On-Premise TCP/SOCKS5 relay paths.
- Avro payloads: small, 1 MB and 2 MB messages.

### Known Limitations at RC1

- mTLS not included.
- OAuth/OAUTHBEARER not included.
- Producer/Receiver role not included (roadmap).
- DLQ publishing not included (roadmap).
