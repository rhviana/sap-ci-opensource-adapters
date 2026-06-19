# SAP CI Open-Source Adapters

[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![SAP Integration Suite](https://img.shields.io/badge/SAP%20Integration%20Suite-Custom%20Adapters-0A6ED1.svg)](#)
[![SAP Cloud Integration](https://img.shields.io/badge/SAP%20Cloud%20Integration-CPI-0A6ED1.svg)](#)
[![Open Source](https://img.shields.io/badge/Open%20Source-Community%20Driven-green.svg)](#)
[![Status](https://img.shields.io/badge/Status-Technical%20Preview-orange.svg)](#)


<img width="1310" height="887" alt="image" src="https://github.com/user-attachments/assets/3a737d09-01ec-4bb9-ba1a-d8696409f22a" />


## Overview

**SAP CI Open-Source Adapters** is an open-source repository dedicated to custom adapter development, extension functionality, reusable integration components, and technical research for **SAP BTP Integration Suite / SAP Cloud Integration**.

The goal of this repository is to explore how SAP Cloud Integration can be extended beyond standard adapter capabilities through community-driven engineering, custom SAP ADK adapters, reusable packages, runtime diagnostics, validation assets, and domain-oriented integration patterns.

This repository is not an official SAP product.

It is an independent open-source technical initiative intended for:

* SAP Cloud Integration custom adapter development
* Extension functionality around SAP standard adapter limitations
* Event-driven architecture experiments
* Runtime diagnostics and troubleshooting patterns
* SAP ADK / Camel-based adapter research
* Open-source reusable integration packages
* Community validation and technical review

All components are provided under the **Apache License 2.0**, in **AS IS** condition, without warranty, SLA, SAP certification, or production-grade claim.

---

## Current Adapter Project

The first public project in this repository is:

## EventSmartKafka

**EventSmartKafka** is a custom Kafka **Consumer Sender Adapter** for SAP Cloud Integration.

It enables Kafka event consumption directly inside SAP CPI, including scenarios where Kafka is available through:

* Cloud Kafka brokers
* On-Premise Kafka
* SAP Cloud Connector TCP/SOCKS5
* SASL / TLS / SCRAM security profiles
* Avro conversion through Schema Registry
* Offset control and recovery behavior

The adapter is delivered as a ready-to-import `.esa` archive, together with source code, documentation, demo iFlow packages, validation scripts, and operational troubleshooting material.

<img width="917" height="511" alt="image" src="https://github.com/user-attachments/assets/c1c3d0cb-ddab-41ad-8cd8-35e84c1a1186" />


---

## Repository Purpose

This repository exists to provide more than isolated source code.

The intended delivery model is a complete technical package:

* Custom adapter source code
* Ready-to-import ESA release artifact
* SAP CPI package assets
* Demo iFlows
* Documentation
* Validation scripts
* Troubleshooting guides
* Performance notes
* SAP Community study-case material
* Reproducible technical baselines

The objective is to help the SAP integration community review, reproduce, challenge, improve, and extend custom adapter capabilities in a transparent way.

---

## Repository Structure

| Path | Purpose |
| ---- | ------- |
| `custom-kafka/` | EventSmartKafka adapter — full project root. |
| `custom-kafka/kafka-local-demo/` | Local Kafka environment: Docker Compose, certs, SASL/TLS properties, Avro schemas, payload binaries, smoke and stress test scripts. |
| `custom-kafka/src/` | Java source code and SAP ADK adapter implementation. |
| `custom-kafka/pom.xml` | Maven build descriptor for the adapter project. |
| `custom-kafka/config.adk` | SAP ADK adapter configuration descriptor. |
| `custom-https/` | Custom HTTPS adapter project (additional adapter). |
| `.gitignore` | Repository-level ignore rules. |
| `CODE_OF_CONDUCT.md` | Community code of conduct. |
| `CONTRIBUTING.md` | Contribution guidelines. |
| `DEIP-SDIA-EIA.md` | Design and architecture reference document. |
| `LICENSE` | Apache License 2.0. |

---

## What This Repository Is

This repository is:

* An open-source SAP Cloud Integration engineering initiative
* A public technical preview workspace
* A custom adapter research and delivery repository
* A place for SAP CPI extension experiments
* A reusable baseline for SAP Community technical review
* A practical study case for event-driven integration in SAP BTP

---

## What This Repository Is Not

This repository is not:

* An official SAP product
* An SAP-certified adapter catalog
* A commercial support channel
* A production SLA offering
* An enterprise certification package
* A replacement for customer-side validation
* A guarantee of compatibility with every CPI runtime or broker environment

Every adopter must validate the adapter and package behavior in their own SAP Integration Suite tenant, broker landscape, security configuration, and operational model.

---

## License

All source code and repository assets are released under the **Apache License 2.0**, unless explicitly stated otherwise in a specific folder.

See:

```text
LICENSE
```

---

## Disclaimer

This project is provided **AS IS**.

There is no warranty, no SLA, no official SAP support, no SAP certification, and no production-readiness guarantee.

The project is intended for technical study, community review, controlled validation, and open-source collaboration.

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

Technical feedback, issue reports, pull requests, validation results, and architecture discussions are welcome.

When reporting issues, include:

* Adapter version
* ESA version
* SAP CPI runtime context
* Broker type
* Security profile
* Proxy type
* Topic
* Group ID
* Payload size
* Error message
* Steps to reproduce

This helps keep discussions technical, reproducible, and useful for the community.
