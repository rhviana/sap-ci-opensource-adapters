# Roadmap — EventSmartKafka

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Status](https://img.shields.io/badge/Status-v1.1.0%20Consumer%20Final-green.svg)](#)

---

## v1.1.0 — Consumer Final ✅ Released

- Consumer Sender Adapter — complete.
- 3,000,000 message stress validation — passed.
- 23 topics · 4 listeners · PLAINTEXT / SASL / SASL_SSL / mTLS.
- On-Premise via Cloud Connector SOCKS5 + OSGi Relay.
- Avro 53 B · 1 MB · 2 MB.
- TIMESEEK single-record and read-all-from-timestamp.
- Parallel Consumers.
- Fail-fast on all error scenarios.
- **No further updates planned for Consumer side.**

---

## Next — Producer / Receiver Adapter

No date committed. Independent open-source project.

Planned scope:
- Kafka Producer Receiver Adapter for SAP CPI.
- Same security profiles as Consumer.
- Cloud + On-Premise.
- Avro serialization.
- Schema Registry integration.

---

## Future (No Date)

- OAuth2 / OAUTHBEARER — requires paid Kafka cloud provider plan.
- Avro / JSON Schema / Protobuf advanced conversion.
- Dead-letter topic publishing.
- Additional domain-governance integration with SDIA / EDCP / ODCP.

---

## Not Planned

SAP certification, enterprise SLA, commercial support, managed service, multi-tenant hosting or vendor benchmark comparison.
