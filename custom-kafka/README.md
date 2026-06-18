# EventSmartKafka — SAP CPI Custom Kafka Consumer Adapter

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-v1.1.0%20Consumer%20Final-green.svg)](#)
[![SAP CPI](https://img.shields.io/badge/SAP%20CPI-Custom%20Adapter-0A6ED1.svg)](#)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-Consumer-black.svg)](#)
[![SAP Cloud Connector](https://img.shields.io/badge/SAP%20Cloud%20Connector-TCP%2FSOCKS5-1f77b4.svg)](#)
[![Avro](https://img.shields.io/badge/Avro-JSON%20%7C%20XML-green.svg)](#)
[![Java](https://img.shields.io/badge/Java-8%20bytecode-red.svg)](#)

**EventSmartKafka** is an open-source custom Kafka **Consumer Sender Adapter** for **SAP BTP Integration Suite / SAP Cloud Integration**.

It was designed to consume Kafka events directly inside CPI, including **On-Premise Kafka through SAP Cloud Connector TCP/SOCKS5**, without exposing the broker publicly, without VPN, and without external bridge middleware.

> **Status:** v1.1.0 — Consumer Final Release. No further updates planned for the Consumer side. Next: Producer/Receiver adapter.
> **License:** Apache License 2.0
> **Warranty:** Provided **AS IS**, without SLA, production warranty, SAP certification, or enterprise certification claim.
> **Support:** This is an independent open-source project. No support, enhancements, bug fixes or SLAs are provided. Use it, fork it, learn from it.

---

## Stress Validation — 3,000,000 Messages

Validated on SAP BTP Trial — Sunday morning pressure stress test.
4 listeners simultaneously. Cloud Connector tunnels. On-Premise via SOCKS5 + OSGi Relay.

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

Validated across 23 topics · 4 listeners · PLAINTEXT / SASL / SASL_SSL / mTLS · Avro payloads 53 B · 1 MB · 2 MB · Apache Kafka 4.1.2 KRaft mode.

---

## What This Repository Contains

| Path | Purpose |
|---|---|
| `documentations` | Documentation, screenshots, validation guides and operational notes. |
| `eventSmartkafka (.esa) releases/` | Ready-to-import SAP CPI custom adapter archive. |
| `pkg-adapter-custom/` | Package containing custom adapter delivery assets. |
| `pkg-iflows-sales-otc/` | Demo package with 29 preconfigured Sales OTC iFlows used in stress tests. |
| `src/main/` | Java source code and SAP ADK adapter implementation. |
| `scripts/` | Validation, smoke and stress test scripts. |

---

## Core Capabilities

- Kafka Consumer Sender Adapter for SAP Cloud Integration.
- Apache Kafka Java Client based consumption.
- On-Premise Kafka via SAP Cloud Connector TCP/SOCKS5 + OSGi Relay.
- Cloud Kafka broker support.
- Security profiles: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL, mTLS. Broker CA exposed only for SASL_SSL.
- SASL mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
- TLS trust sources: JVM default truststore and CPI keystore certificate alias for private broker CA.
- Avro conversion: Magic Byte, Fixed Schema ID, Avro to JSON, Avro to XML.
- Offset control: EARLIEST, LATEST, NONE.
- Error handling: Stop on Error, Skip Failed Message, Retry Failed Message.
- TIMESEEK recovery by timestamp — single record or read-all-from-timestamp.
- Parallel consumers support — N consumer instances sharing same group.id.
- New group detection warning when auto.offset.reset=LATEST and no committed offsets.
- Fail-fast on all configuration errors — no infinite loops, no zombie adapters.
- CPI diagnostic headers using `x-sdiakafka-*`.

---

## Architecture Summary

```
SAP CPI Worker
  -> EventSmartKafka Sender Adapter
       -> Apache Kafka Consumer
            -> Local TCP Relay: 127.0.0.1:<port>
                 -> SAP Connectivity SOCKS5 Proxy
                      -> SAP Cloud Connector
                           -> On-Premise Kafka Broker
```

---

## Quick Start

1. Import the `.esa` adapter into SAP Integration Suite.
2. Import the demo iFlow package.
3. Configure Security Material aliases.
4. Configure Kafka bootstrap, topic and group ID.
5. Configure security profile and TLS trust source.
6. For On-Premise mode, configure SAP Cloud Connector TCP mappings.
7. Deploy the iFlows.
8. **Redeploy all iFlows after installing a new adapter ESA version.**
9. Produce Kafka messages and validate CPI monitoring.

---

## Important — Group ID and Auto Offset Reset

When using a new Group ID, if `auto.offset.reset=LATEST`, messages already in the topic will not be read.
The adapter will log a clear warning: `⚠️ NEW GROUP DETECTED`.
Change to `EARLIEST` if you want to read existing messages, then redeploy.

---

## Parallel Consumers

Set `Parallel Consumers > 1` to create multiple consumer instances sharing the same `group.id`.
Kafka distributes partitions among them automatically via group rebalance.

Rules:
- `Parallel Consumers` cannot be combined with explicit `Partition` configuration — adapter will fail fast.
- Effective parallelism is bounded by the number of topic partitions.
- `Parallel Consumers > number of partitions` → idle consumers, warning logged.

---

## Apache 2.0 License

This project is released under the **Apache License 2.0**. See `LICENSE`.

---

## Author

**Ricardo Luz Holanda Viana**
Independent Integration Architecture Researcher
SAP BTP Integration Suite Expert Developer
SAP Press Author — Enterprise Messaging, SAP Press 2021
Creator of DEIP · SDIA · EDCP · ODCP · GDCR
ORCID: `0009-0009-9549-5862`

GitHub: https://github.com/rhviana/sap-ci-opensource-adapters/tree/main/custom-kafka
