<div align="center">

# SAP CI Open Source Adapters

**A community collection of open-source custom adapters for SAP Cloud Integration (CPI), built with the SAP Adapter Development Kit (ADK).**

[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](./LICENSE)
[![SAP](https://img.shields.io/badge/SAP-Integration_Suite-0FAAFF.svg)](https://www.sap.com/products/technology-platform/integration-suite.html)
[![Status](https://img.shields.io/badge/Status-Experimental_Preview-orange.svg)](#status)

</div>

---

## Overview

SAP adapters are often treated as black boxes. Developers configure them, deploy iFlows, and wait for messages to arrive — but very few see what happens inside the adapter runtime.

This repository opens that box. It is a growing collection of **open-source, deployable custom adapters** for SAP Integration Suite, each engineered to a production-minded standard and documented in depth.

The goal is educational and practical: to help the SAP Community understand how a serious custom adapter is built — metadata, endpoint, consumer lifecycle, security profiles, connectivity, error handling, and offset control — and to provide working code that can be deployed and studied.

---

## Available Adapters

| Adapter | Description | Version | Documentation |
|---------|-------------|---------|---------------|
| 🟢 [**Kafka**](./kafka) | Apache Kafka consumer adapter — SASL/TLS security profiles, Cloud Connector TCP relay (no broker internet exposure), offset lifecycle control, Avro / Schema Registry conversion, and message recovery by timestamp. | `v1.0.0` | [kafka/README.md](./kafka/README.md) |

> More adapters will be added over time. Each lives in its own top-level folder with independent documentation, build, and release.

---

## Status

All adapters in this repository are released as **Community Demo / Experimental Technical Preview**.

They are:

- ❌ Not SAP-certified
- ❌ Not SLA-backed
- ❌ Not enterprise stress-tested
- ❌ Not a replacement for vendor-supported adapters

✅ They **are** functional, deployable, and validated in controlled non-production environments.

**Production usage requires independent validation by the adopting organization.**

---

## Repository Structure

```
sap-ci-opensource-adapters/
├── LICENSE                   Apache License 2.0 — applies to all adapters
├── README.md                 This file — suite index
├── CONTRIBUTING.md           Contribution rules (no SAP JARs, no secrets)
├── CODE_OF_CONDUCT.md        Contributor Covenant 2.1
├── .gitignore                Excludes SAP JARs, keystores, build output
├── .github/
│   └── workflows/
│       └── kafka-build.yml    CI — path-filtered, runs only on kafka/ changes
└── kafka/                    EventSmartKafka adapter
    ├── README.md             Full adapter documentation
    ├── CHANGELOG.md
    ├── pom.xml
    ├── config.adk
    └── src/...               Source (Java + ADK metadata)
```

---

## Getting Started

### Use a pre-built adapter (no build required)

1. Open the [**Releases**](../../releases) page
2. Download the `.esa` for the adapter and version you need
3. In SAP Integration Suite: **Design → Import → Integration Adapter**
4. Select the `.esa` — the adapter is now available in your tenant

Release tags are namespaced per adapter, e.g. `kafka-v1.0.0`.

### Build from source

Each adapter is a self-contained Maven project. To build the Kafka adapter:

```bash
git clone https://github.com/rhviana/sap-ci-opensource-adapters.git
cd sap-ci-opensource-adapters/kafka
mvn clean install
```

The SAP ADK APIs are pulled from **Maven Central** — no local SAP JARs are required. See each adapter's README for full prerequisites.

---

## License & SAP Dependencies

All code in this repository is licensed under the [**Apache License 2.0**](./LICENSE), which includes an explicit patent grant. You are free to use, modify, and distribute this software, including commercially, under the license terms.

**This repository does not redistribute any SAP-owned JARs.** The SAP ADK APIs are declared as Maven dependencies and downloaded from Maven Central at build time, under SAP's own license terms. Your own adapter code is your intellectual property.

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](./CONTRIBUTING.md) and the [Code of Conduct](./CODE_OF_CONDUCT.md) before opening a pull request.

Two ground rules above all:

1. **Never commit SAP-owned JARs** (`compile-libs/`, `libs/`, `component/`, `com.sap.it.*`)
2. **Never commit secrets** (`.p12`, `.jks`, `.pem`, `.key`, credentials)

---

## About

Created and maintained by **Ricardo Viana** — Independent Solo Researcher, Enterprise Integration Architecture.

This work accompanies a technical blog series on the SAP Community explaining the engineering behind each adapter, and is part of the **SDIA (Semantic Domain Integration Architecture)** body of work.

- 🌐 SAP Community: [community.sap.com](https://community.sap.com)
- 🆔 ORCID: [0009-0009-9549-5862](https://orcid.org/0009-0009-9549-5862)

---

<div align="center">

*Technology executes, but the domain governs.*

</div>
