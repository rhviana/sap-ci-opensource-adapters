# Changelog

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


All notable changes to this project are documented here.

---

## [1.0.0] — 2026-06-10

### Added
- SAP Cloud Connector TCP tunnel (`SdiaCloudConnectorTcpTunnel`) — custom SOCKS5 relay enabling raw Kafka binary protocol through SAP Connectivity. SAP does not provide this via ADK.
- SAP proprietary SOCKS5 auth method `0x80` (JWT) with RFC 1929 fallback. `NO_AUTH` is disabled by default and requires an explicit JVM property for controlled test topologies.
- Multi-broker multi-tunnel support — one relay per bootstrap port, passive reuse across iFlows in the same CPI runtime.
- Fail-fast probe (`SdiaCloudConnectorTcpProbe`) — validates the SOCKS5 path before Kafka starts, with automatic candidate port resolution from environment variables and `CloudConnectorProperties`.
- Kafka consumer with full offset lifecycle: manual commit, `earliest`/`latest`/`none` auto-offset reset, explicit partition assignment.
- Error handling policies: Stop on Error, Skip Failed Message (poison-pill commit), Retry Failed Message (exponential backoff, 5–10 attempts).
- Message Recovery by timestamp: seek to epoch ms/s or ISO-8601 UTC, single-message isolation mode, original offset restoration after isolated replay.
- Avro / Schema Registry conversion: Confluent wire format (magic byte), fixed schema ID, Avro → JSON, Avro → XML.
- Schema cache: TTL 1 hour, LRU eviction at 256 entries, parsed `AvroSchema` tree cached alongside JSON (zero allocation per message on cache hit).
- Hard payload limits: 5 MB for Avro conversion, 20 MB for raw pass-through.
- Schema JSON size limit: 1–100 KB configurable, 100 KB hard ceiling.
- Phase 1 security matrix: NONE, SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512, PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL, public CA and private broker CA from SAP CPI Keystore. OAuth/OAUTHBEARER, mTLS/client certificate and Kerberos/GSSAPI are blocked by design.
- Zero-dependency Avro decoder and JSON parser (no Avro/Jackson libraries in the OSGi bundle).
- `SdiaKafkaEndpointInformationService` — exposes consumer status in CPI Operations Monitor.

### Security
- Bearer JWT is converted to `byte[]` in `ProxyConfig`; per-handshake copies are zeroed immediately after use.
- Password `char[]` from `SecureStoreService` zeroed immediately after `String` construction.
- `ConcurrentHashMap.computeIfAbsent` replaces `synchronized(alias.intern())` in `SdiaKafkaCredentialsResolver` — eliminates string pool pinning in OSGi.
- Secret and payload-body logging reduced/redacted; error exchanges carry payload size and SHA-256 instead of raw payload body.

### Performance
- Block-copy escape in `escapeJson` and `escapeXml` — bulk-copies safe character runs instead of one `append` per character.
- `toXmlName` fast-path — returns original `String` with zero allocation when the name is already a valid XML element name.
- Manual nibble decode for `\uXXXX` JSON escapes — no `substring` allocation, no `parseInt`.
- Pipe buffer size matched to socket buffer size (64 KB) — one syscall per full OS buffer.
- `flush()` removed from relay `Pipe` hot path — `TCP_NODELAY` handles low-latency delivery.
- Bounded `ThreadPoolExecutor` (max 32 workers per port) replaces unbounded `new Thread()` per connection.
- `setPerformancePreferences(0, 1, 2)` on relay sockets — favours latency over connection time and bandwidth.
- Per-poll and per-record hot-path log statements removed. Remaining 45 `LOG.warn/error` calls fire only at startup, on failures, or on a 30-second throttled heartbeat.
- Static `Pattern` for digits-only check in timestamp parsing — no per-call compile.
