
[![License: Apache 2.0](https://img.shields.io/badge/Code%20License-Apache%202.0-blue.svg)](LICENSE)
[![Architecture License: CC BY 4.0](https://img.shields.io/badge/Architecture%20Publications-CC%20BY%204.0-green.svg)](https://github.com/rhviana/deip)
[![DEIP](https://img.shields.io/badge/DEIP-Domain%20Enterprise%20Integration%20Pattern-0B3D91.svg)](https://github.com/rhviana/deip)
[![SDIA](https://img.shields.io/badge/SDIA-Semantic%20Domain%20Integration%20Architecture-0A6ED1.svg)](https://github.com/rhviana/deip)
[![ODCP](https://img.shields.io/badge/ODCP-Orchestration%20Domain--Centric%20Pattern-1B5E20.svg)](https://github.com/rhviana/deip)
[![EDCP](https://img.shields.io/badge/EDCP-Event%20Domain--Centric%20Pattern-6A1B9A.svg)](https://github.com/rhviana/deip)
[![EventSmartKafka](https://img.shields.io/badge/EventSmartKafka-SAP%20CPI%20Kafka%20Adapter-orange.svg)](#)

<img width="911" height="502" alt="image" src="https://github.com/user-attachments/assets/8c7a1db7-29ac-4069-bc3d-522cdcaf95d0" />

---

## Purpose

This document explains how this repository connects to the broader **DEIP / SDIA ecosystem**.

The short version:

```text
DEIP / SDIA defines the semantic governance model.
EventSmartKafka provides a practical SAP CPI runtime implementation example.
```

This repository contains executable open-source adapter assets for SAP Cloud Integration.

The DEIP repository contains the architectural foundation, prior-art publications, semantic governance models, and domain-centric integration patterns.

Reference repository:

```text
https://github.com/rhviana/deip
```

---

## License Boundary

This repository and its source code are released under:

```text
Apache License 2.0
```

The architectural publications and prior-art documents in the DEIP / SDIA ecosystem are published as open architecture references under:

```text
CC BY 4.0
```

This separation is intentional:

| Area | License / Status |
|---|---|
| EventSmartKafka source code and adapter assets | Apache License 2.0 |
| DEIP / SDIA architectural publications | CC BY 4.0 |
| SAP CPI demo packages and examples | Apache License 2.0 unless stated otherwise |
| Diagrams, technical notes and architecture explanations | CC BY 4.0 where explicitly referenced |

---

## What DEIP Provides

**DEIP — Domain Enterprise Integration Pattern** defines the semantic control-plane decision model for enterprise integration governance.

DEIP organizes integration decisions through a structured pipeline:

```text
Intent
  -> Interaction Type
      -> Quality Profile
          -> Integration Capability
              -> Protocol Profile
                  -> Platform Binding
```

DEIP answers a strategic question:

```text
When should a platform, protocol, pattern or runtime capability exist?
```

In the broader ecosystem, DEIP is the decision foundation that governs semantic continuity across integration layers.

Primary reference:

```text
DEIP — Domain Enterprise Integration Pattern
Ecosystem DOI: 10.5281/zenodo.19004802
Repository: https://github.com/rhviana/deip
```

<img width="832" height="507" alt="image" src="https://github.com/user-attachments/assets/77ff34e0-05cb-40c7-8c0b-186766324183" />


---

## What SDIA Provides

**SDIA — Semantic Domain Integration Architecture** is the umbrella architecture of the DEIP ecosystem.

SDIA defines the taxonomy, ontology and implicit metamodel required to preserve domain semantics across the integration stack.

Its core invariant is:

```text
The domain is the primary key.
```

The same domain token should survive across gateway, resolution, orchestration, event and data layers.

Example:

```text
acme.sales.otc
```

This domain token may appear in:

```text
API facade
runtime routing key
CPI package
iFlow name
Kafka topic
consumer group
schema subject
credential alias
data object
```

The technology may change. The domain grammar remains stable.

Primary reference:

```text
SDIA — Semantic Domain Integration Architecture
Zenodo: https://doi.org/10.5281/zenodo.18877635
Repository: https://github.com/rhviana/deip
```

---

## SDIA Runtime Layers

The SDIA ecosystem is organized into semantic layers.

| Layer | Component | Role |
|---|---|---|
| Layer 1 | GDCR | Gateway semantic facade and metadata-driven API routing. |
| Layer 2 | DDCR | Deterministic runtime semantic resolution engine. |
| Layer 3 | ODCP | Domain-centric orchestration topology for packages, iFlows, credentials and channels. |
| Layer 4 | EDCP | Domain-centric event topology for topics, queues, event meshes and consumer groups. |
| Layer 5 | DDCP | Domain-centric persistent data topology. |

<img width="1297" height="837" alt="image" src="https://github.com/user-attachments/assets/a0a2696a-42a1-4cec-8ed7-56f53ca811ca" />

---

## GDCR — Gateway Layer

**GDCR — Gateway Domain-Centric Routing** defines the semantic facade model for API gateways.

Its principle:

```text
One proxy per domain, not one proxy per backend.
```

GDCR separates the stable consumer-facing facade from mutable backend resolution.

Reference:

```text
GDCR — Gateway Domain-Centric Routing
Zenodo: https://doi.org/10.5281/zenodo.18582492
Repository: https://github.com/rhviana/deip
```

---

## DDCR — Runtime Resolution Layer

**DDCR — Domain Driven-Centric Router** is the deterministic runtime semantic resolution layer.

It resolves semantic intent into backend targets through a fixed pipeline.

Its principle:

```text
The engine never guesses.
It resolves or fails fast.
```

Reference:

```text
DDCR — Deterministic Runtime Semantic Resolution Engine
Zenodo: https://doi.org/10.5281/zenodo.18864832
Repository: https://github.com/rhviana/deip
```

---

## ODCP — Orchestration Layer

**ODCP — Orchestration Domain-Centric Pattern** governs orchestration artifacts.

It covers:

- packages;
- iFlows;
- channels;
- credentials;
- certificates;
- value mappings;
- message mappings;
- script collections;
- data stores;
- variables;
- runtime ownership.

For SAP Cloud Integration, ODCP keeps CPI artifacts aligned to the same domain grammar.

Example:

```text
acme.br.sales.otc.i
id00.otc.kafka.ordercreated.cpi.none.plaintext.op
cr.sales.otc.basic.kafka
```

Reference:

```text
ODCP — Orchestration Domain-Centric Pattern
Zenodo: https://doi.org/10.5281/zenodo.18876593
Repository: https://github.com/rhviana/deip
```

---

## EDCP — Event Layer

**EDCP — Event Domain-Centric Pattern** governs event topology.

It covers:

- Kafka topics;
- consumer groups;
- queues;
- event mesh subjects;
- MQTT topics;
- schema registry subjects;
- broker artifacts;
- event ownership;
- event lifecycle governance.

For EventSmartKafka, EDCP is the semantic layer that explains why Kafka topics and consumer groups should be named by business domain rather than by random technology or team conventions.

Example:

```text
acme.sales.otc.order.created
acme.sales.otc.invoice.created
cg.id25.acme.sales.otc.orders.created.2mb
```

Reference:

```text
EDCP — Event Domain-Centric Pattern
Zenodo: https://doi.org/10.5281/zenodo.19068766
Repository: https://github.com/rhviana/deip
```

---

## DDCP — Data Layer

**DDCP — Data Domain-Centric Pattern** governs persistent data topology.

It covers:

- databases;
- schemas;
- tables;
- collections;
- buckets;
- cache namespaces;
- analytical datasets;
- snapshots.

Its role is to keep persistent data artifacts aligned to the same domain grammar used by gateway, runtime, orchestration and event layers.

Reference:

```text
DDCP — Data Domain-Centric Pattern
Zenodo: https://doi.org/10.5281/zenodo.19730519
Repository: https://github.com/rhviana/deip
```

---

## How EventSmartKafka Fits

**EventSmartKafka** is not the DEIP ecosystem itself.

It is a practical SAP CPI implementation that demonstrates how the DEIP / SDIA ideas can be applied to a real runtime component.

EventSmartKafka connects mainly to:

| Ecosystem Area | EventSmartKafka Connection |
|---|---|
| SDIA | Uses domain grammar as the semantic backbone. |
| ODCP | Organizes CPI packages, iFlows, credentials and adapter artifacts. |
| EDCP | Governs Kafka topics, consumer groups and event names. |
| DDCR | Influences metadata-driven and deterministic runtime thinking. |
| DEIP | Provides the higher-level decision logic for when this capability should exist. |

---

## Example: Sales OTC Semantic Continuity

The same semantic boundary can appear across multiple artifact types:

| Artifact Type | Example |
|---|---|
| Domain grammar | `acme.sales.otc` |
| Kafka topic | `acme.sales.otc.order.created` |
| CPI package | `acme.br.sales.otc.i` |
| iFlow | `id00.otc.kafka.ordercreated.cpi.none.plaintext.op` |
| Credential alias | `cr.sales.otc.basic.kafka` |
| Schema Registry credential | `cr.sales.otc.schema.confluent.kafka` |
| Consumer group | `cg.id25.acme.sales.otc.orders.created.2mb` |

This is the practical meaning of:

```text
One Domain.
One Grammar.
Multiple Artifacts.
```

---

## Why This Matters

Most integration landscapes grow through local technical decisions:

```text
one topic here
one package there
one credential somewhere else
one route inside a script
one mapping hidden in an iFlow
one consumer group with no semantic context
```

The result is usually technical sprawl.

DEIP / SDIA provides the governance model.

EventSmartKafka provides one executable implementation example inside SAP Cloud Integration.

The objective is not only to connect Kafka to CPI.

The objective is to prove that event-driven integration can be:

- governable;
- traceable;
- reproducible;
- observable;
- domain-aligned;
- open-source;
- technically reviewable.

<img width="1567" height="881" alt="image" src="https://github.com/user-attachments/assets/0c829b83-ce1e-414a-a4fe-bd13319fd114" />

---

## Relationship Between Repositories

| Repository | Purpose |
|---|---|
| `github.com/rhviana/deip` | Architectural source of truth for DEIP, SDIA, GDCR, DDCR, ODCP, EDCP and DDCP. |
| `sap-ci-opensource-adapters/custom-kafka` | Executable SAP CPI adapter implementation and demo assets for EventSmartKafka. |

Use the DEIP repository to understand the architecture.

Use this repository to test and review the adapter implementation.

---

## Publication References

Primary ecosystem repository:

```text
https://github.com/rhviana/deip
```

Core publications:

```text

DEIP — Domain Enterprise Integration Pattern
https://doi.org/10.5281/zenodo.19004802

SDIA — Semantic Domain Integration Architecture
https://doi.org/10.5281/zenodo.18877635

GDCR — Gateway Domain-Centric Routing
https://doi.org/10.5281/zenodo.18582492

DDCR — Domain Driven-Centric Router
https://doi.org/10.5281/zenodo.18864832

ODCP — Orchestration Domain-Centric Pattern
https://doi.org/10.5281/zenodo.18876593

EDCP — Event Domain-Centric Pattern
https://doi.org/10.5281/zenodo.19068766

DDCP — Data Domain-Centric Pattern
https://doi.org/10.5281/zenodo.19730519
```

---

## Status

The DEIP / SDIA ecosystem publications are structurally frozen as public prior-art and architecture references.

EventSmartKafka remains an executable open-source adapter project and community validation baseline.

This repository does not replace the DEIP repository.

It operationalizes part of the model through a concrete SAP CPI Kafka adapter.


---

## www.domain-intent.com

The the website with resume of especification and name convention generators, under adjustments.


---
## Author

**Ricardo Luz Holanda Viana**  
Independent Integration Architecture Researcher  
SAP BTP Integration Suite Expert Developer  
SAP Press Author — Enterprise Messaging, SAP Press 2021  
Creator of DEIP · SDIA · GDCR · DDCR · ODCP · EDCP · DDCP  
ORCID: `0009-0009-9549-5862`

---

## License Notice

EventSmartKafka source code and adapter assets are released under the **Apache License 2.0**.

DEIP / SDIA ecosystem publications are released as architectural publications under **CC BY 4.0**, as stated in the corresponding publication records.

This file is an explanatory bridge between the executable adapter repository and the architectural ecosystem.
