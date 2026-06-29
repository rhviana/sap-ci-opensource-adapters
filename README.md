# SAP CI Open-Source Adapters

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![SAP Integration Suite](https://img.shields.io/badge/SAP%20Integration%20Suite-Custom%20Adapters-0A6ED1.svg)](#)
[![SAP Cloud Integration](https://img.shields.io/badge/SAP%20Cloud%20Integration-CPI-0A6ED1.svg)](#)
[![Open Source](https://img.shields.io/badge/Open%20Source-Community%20Driven-green.svg)](#)
[![Status](https://img.shields.io/badge/Status-v1.1.0%20Consumer%20Final-green.svg)](#)

## Overview

**SAP CI Open-Source Adapters** is an open-source repository dedicated to custom adapter development, extension functionality, reusable integration components, and technical research for **SAP BTP Integration Suite / SAP Cloud Integration**.

The goal of this repository is to explore how SAP Cloud Integration can be extended beyond standard adapter capabilities through community-driven engineering, custom SAP ADK adapters, reusable packages, runtime diagnostics, validation assets, and domain-oriented integration patterns.

This repository is not an official SAP product. It is an independent open-source technical initiative intended for:

- SAP Cloud Integration custom adapter development
- Extension functionality around SAP standard adapter limitations
- Event-driven architecture experiments
- Runtime diagnostics and troubleshooting patterns
- SAP ADK / Camel-based adapter research
- Open-source reusable integration packages
- Community validation and technical review

All components are provided under the **Apache License 2.0**, in **AS IS** condition, without warranty, SLA, SAP certification, or production-grade claim.

> **No support is provided.** This is an independent open-source project. No bug fixes, enhancements, or SLAs. Use it, fork it, learn from it.

---

## EventSmartKafka — Consumer Final v1.1.0

**EventSmartKafka** is a custom Kafka **Consumer Sender Adapter** for SAP Cloud Integration.

It enables Kafka event consumption directly inside SAP CPI, including On-Premise Kafka through SAP Cloud Connector TCP/SOCKS5, without exposing the broker publicly, without VPN, and without external bridge middleware.

### Stress Validation — 4.5 Million Messages Total

| Cycle | Messages | Failed | Result |
|---|---|---|---|
| RC1 validation | 1,500,000 | 0 | ✅ PASS |
| v1.1.0 stress test | 3,000,000 | 0 | ✅ PASS |
| **Total** | **4,500,000** | **0** | **✅ PASS** |

v1.1.0 stress test details — SAP BTP Trial, Sunday morning pressure test:

| Metric | Result |
|---|---|
| Total messages | 3,000,000 |
| Total failed | 0 |
| Avg throughput | 1,825 msg/s |
| Avg bandwidth | 1.88 MiB/s |
| Total data | 3.023 GiB |
| Elapsed | 00:27:23 |
| CPI Completed | 3,000,023 |
| CPI Failed | 0 |
| CPI Retry | 0 |

23 topics · 4 listeners · PLAINTEXT / SASL / SASL_SSL / mTLS · Avro 53 B · 1 MB · 2 MB · Apache Kafka 4.1.2 KRaft mode · 29 iFlows.

### Capabilities

- Kafka Consumer Sender Adapter for SAP Cloud Integration
- On-Premise Kafka via SAP Cloud Connector TCP/SOCKS5 + OSGi Relay
- Cloud Kafka broker support
- Security: PLAINTEXT, SASL_PLAINTEXT, SASL_SSL, mTLS
- SASL mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
- TLS: JVM trust store or custom CA alias (SASL_SSL only)
- Avro to JSON / Avro to XML conversion
- Confluent Magic Byte / Wire Format + Fixed Schema ID
- Schema Registry cache and timeout handling
- Offset control: EARLIEST, LATEST, NONE
- TIMESEEK recovery — single record or read-all-from-timestamp
- Parallel consumers — N instances, Kafka distributes partitions automatically
- Fail-fast on all error scenarios — no infinite loops, no zombie adapters
- New consumer group detection with LATEST warning
- Structured error messages with topic, partition, offset, UTC timestamp, Unix epoch and TIMESEEK hint

### Status

**v1.1.0 — Consumer Final Release.**
No further updates planned for the Consumer/Sender side.
**Next: Producer/Receiver adapter.**

<img width="917" height="511" alt="image" src="https://github.com/user-attachments/assets/c1c3d0cb-ddab-41ad-8cd8-35e84c1a1186" />

---

## Repository Structure

| Path | Purpose |
|---|---|
| `custom-kafka/` | EventSmartKafka adapter — full project root. |
| `custom-kafka/src/` | Java source code and SAP ADK adapter implementation. |
| `custom-kafka/kafka-local-demo/` | Local Kafka environment: Docker Compose, certs, SASL/TLS properties, Avro schemas, smoke and stress test scripts. |
| `custom-kafka/eventSmartKafka (.esa) releases/` | Ready-to-import SAP CPI custom adapter archive. |
| `custom-kafka/pkg-iflows-sales-otc/` | Demo package with 29 preconfigured Sales OTC iFlows. |
| `custom-kafka/pom.xml` | Maven build descriptor. |
| `custom-kafka/config.adk` | SAP ADK adapter configuration descriptor. |
| `custom-https/` | Custom HTTPS adapter project — next release. |
| `DEIP-SDIA-EIA.md` | Design and architecture reference document. |
| `CODE_OF_CONDUCT.md` | Community code of conduct. |
| `CONTRIBUTING.md` | Contribution guidelines. |
| `LICENSE` | Apache License 2.0. |

---

## What This Repository Is

- An open-source SAP Cloud Integration engineering initiative
- A public technical preview workspace
- A custom adapter research and delivery repository
- A practical study case for event-driven integration in SAP BTP
- A reusable baseline for SAP Community technical review

## What This Repository Is Not

- An official SAP product
- An SAP-certified adapter catalog
- A commercial support channel
- A production SLA offering
- An enterprise certification package

Every adopter must validate adapter behavior in their own SAP Integration Suite tenant, broker landscape, security configuration, and operational model.

---

## License

All source code and repository assets are released under the **Apache License 2.0**.

---

## Disclaimer

This project is provided **AS IS**. No warranty, no SLA, no official SAP support, no SAP certification, no production-readiness guarantee. Independent open-source project for technical study, community review, and controlled validation.

---

## Author

**Ricardo Luz Holanda Viana**
Independent Integration Architecture Researcher
SAP BTP Integration Suite Expert Developer
SAP Press Author — Enterprise Messaging, SAP Press 2021
Creator of DEIP · SDIA · EDCP · ODCP · GDCR
ORCID: `0009-0009-9549-5862`

---

## Community Review

Technical feedback, issue reports, pull requests and validation results are welcome.

When reporting issues, include: adapter version · ESA version · SAP CPI runtime · broker type · security profile · proxy type · topic · group ID · payload size · error message · steps to reproduce.
