## v3.0.0 — Ricardoviana Runtime Guard


- Aligned ADK metadata/config/POM version to 3.0.0 and vendor to ricardoviana.opensource.
- Kept Consumer-only runtime and existing consumer diagnostic messages.
- Hardened Cloud Connector relay thread scheduling and socket limits.
- Added XML table positional-cell fallback for bootstrap host/port parsing.
- Wired Schema Registry connect/read timeout metadata into the HTTP client.
- Preserved CPI-safe Camel 3.14 default; added Camel 4.4 lab profile only.


## v2.0.3 — Avro Schema/Payload Guard Hotfix

- Added Magic Byte expected Schema ID guard: when schemaRegistrySchemaId is filled in Magic Byte mode, the adapter verifies the payload wire header Schema ID matches it.
- Added empty conversion guard: Avro conversion cannot pass a blank body silently.
- Raised effective Kafka fetch floor while Avro conversion is enabled so oversized records can be fetched and rejected visibly instead of producing empty polls.
- Broadened Avro RuntimeCamelException detection so adapter-generated Avro failures do not get reclassified as generic pipeline failures.

# Changelog

## v2.0.1 — Critical bootstrap resolver fix

### Fixed
- **CRITICAL**: `resolveBootstrapServers()` failed to parse `kafkaClusterHostsTable` when the SAP ADK runtime serialized table rows as `<row index="0">` or `<row id="...">` (with attributes) instead of bare `<row>`. The old parser used `indexOf("<row>", ...)` which never matched tags with attributes, silently returning `null` and producing `Event.Smart.Kafka.Config.Broker.Resolve.Failed` even when the host/port table was filled in correctly in the CPI UI. The parser now searches for `<row` (without the closing `>`) so any attributes on the tag no longer break detection.
- **CRITICAL**: `normalizeSchemaCacheKb()` clamped to `50/100/250` KB — leftover from a previous metadata version. Current metadata only allows `5/10/15` KB; this mismatch meant the runtime schema cache size never matched what the user selected in the UI. Now clamps to `5/10/15` KB matching `schemaIdBufferSize` FixedValues.
- **CRITICAL**: `normalizePayloadMb()` clamped to `1/3/5` MB — leftover from a previous metadata version. Current metadata only allows `1/2` MB MAX; selecting `2` MB silently promoted to `3` MB at runtime. Now clamps to `1/2` MB matching `maxPayloadConversionSize` FixedValues.
- `schemaIdBufferSize` Java field default was `"50"`, inconsistent with the `5/10/15` KB FixedValues — same root cause class as the metadata.xml `<Default>50</Default>` bug fixed earlier. Now defaults to `"5"`.
- `extractXmlCellValue()` had the same attribute-intolerance bug as the row scanner — hardened to always locate the opening tag's closing `>` before reading cell content, regardless of extra attributes.
- Added a JSON fallback path that finds the first `{` or `[` anywhere in the string (not just at index 0), accepting bare single-object serializations some ADK runtime versions emit for single-row tables.
- Added a plain-text `host:port` fallback for ADK runtime versions that serialize a single-row table as a flat string with no XML/JSON wrapper.
- When no known format matches, the resolver now logs a `WARN` with the raw value length and an 80-character preview instead of failing silently — diagnosable directly from CPI trace logs.

### Verified (no bug found)
- Avro payload conversion limit confirmed at 1 MB / 2 MB MAX (`maxPayloadConversionSize`).
- Schema Registry cache size limit confirmed at 5 / 10 / 15 KB MAX (`schemaIdBufferSize`), now actually enforced after the fix above.
- Fetch size limit confirmed at 1–20 MB (`maxFetchSize`) — governs raw Kafka record fetch size, independent from the Avro conversion limit.
- "Fixed Schema ID" mode without a configured schema ID throws a clear `IllegalArgumentException` both at channel validation time and at conversion time. No silent failure or `NullPointerException` path found.

## v2.0.0

### Breaking changes
- **Consumer-only** — `KafkaProducerFactory` and `SdiaKafkaProducer` removed. Producer will be released separately.
- **Authentication** — mTLS (`Client Certificate`) and OAuth 2.0 removed. Supported: `NONE` and `SASL` (PLAIN · SCRAM-SHA-256 · SCRAM-SHA-512).
- **Port field** — `kafkaPortRow` changed from `xsd:integer` to `xsd:string`. CPI external parameters can now bind to the port field.
- **Retry** — options reduced to `5` (default) and `10` (maximum). Values 6–9 removed from metadata.
- **Payload limits** — Avro conversion capped at 1 MB / 2 MB MAX. Schema size capped at 5 / 10 / 15 KB MAX.
- **Dependencies** — `compile-libs/` folder and all local `scope=system` JARs removed. All dependencies now resolve from Maven Central.

### New
- SAP ADK upgraded to `2.27.0` (from `2.25.0`).
- Kafka clients upgraded to `3.6.2` (from `3.4.0`).
- `adapter.api` added as explicit Maven Central dependency.
- Schema Registry cache upgraded to **1-hour TTL** with `CacheEntry` expiry tracking and smart eviction (expired-first before full clear). Cache capacity increased to 512 entries.
- Schema Registry HTTP: `Connection: keep-alive`, `Accept-Encoding: gzip`, buffer 8 KB (was 2 KB), connect timeout 3 s (was 5 s), read timeout 8 s (was 10 s).
- OAuth `applyOAuth` method removed from `KafkaConsumerFactory`.
- All development/debug `.txt` and `.md` notes removed from repository.

## v1.0.0

- Initial release: Consumer adapter with NONE, SASL, mTLS, OAuth profiles.
- SAP Cloud Connector TCP/SOCKS5 tunnel with SAP JWT method 0x80 and RFC1929 fallback.
- Avro conversion: Magic Byte and Fixed Schema ID modes.
- Offset control: Earliest, Latest, None, Seek by Timestamp, Message Recovery.
- Error handling: Skip, Stop on Error, Retry (5–10 attempts).
- Poison Pill skip with silent commit.
- Validated: 498,516 messages, 0 failures, 0 retries across 20 iFlows and 3 CC tunnels simultaneously.
