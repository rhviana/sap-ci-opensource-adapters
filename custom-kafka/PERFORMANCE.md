# Performance

## Validated Results

| Test | Messages | Failed | Retries | Result |
|---|---|---|---|---|
| Milestone 1 | 498,516 | 0 | 0 | ✅ |
| Milestone 2 | 800,000 | — | — | 🔄 In progress |

**Setup:** 20 iFlows · 3 SAP Cloud Connector TCP/SOCKS5 tunnels · 3 ports (19091 PLAINTEXT, 19092 SASL, 19093 SASL+TLS) · payloads 53 B · 1 MB · 2 MB · Avro Magic Byte · Avro Fixed Schema ID.

---

## Architecture Decisions

### TCP Relay (SdiaCloudConnectorTcpTunnel)
- Local `ServerSocket` binds on `127.0.0.1:<brokerPort>` — Kafka client connects locally, relay forwards via SOCKS5.
- Pipe buffer = socket buffer = **64 KB** — one `read()` syscall drains the OS receive buffer.
- No `flush()` in pipe hot path — `TCP_NODELAY` handles low-latency delivery without an extra syscall per write.
- Worker threads from a bounded `ThreadPoolExecutor` (max 32 per port) with 60 s keep-alive — no unbounded thread creation under connection storms.
- `setPerformancePreferences(0, 1, 2)` on all sockets — JVM hint to favour latency over connection time and bandwidth.
- Bearer JWT stored as `byte[]`, zeroed after use — not a `String` (prevents heap-dump token exposure).
- Passive reuse: if the relay port is already bound by another iFlow in the same CPI runtime, subsequent iFlows attach without creating new threads or sockets.

### Schema Registry Cache (SdiaSchemaRegistryClient)
- `ConcurrentHashMap` with `CacheEntry` (schema + expiry timestamp).
- TTL = **1 hour** — schemas are immutable in Confluent Schema Registry; 1 hour is safe.
- Eviction: expired entries removed first on overflow; full clear only as last resort.
- Max entries: 512.
- HTTP: `Connection: keep-alive`, `Accept-Encoding: gzip`, 8 KB read buffer, 3 s connect / 8 s read timeout.

### Avro Decoder (SdiaKafkaAvroConverter)
- Zero external dependency — no Avro library in the OSGi bundle.
- `AvroDecoder` reads directly from `byte[]` with a position pointer — no `InputStream`, no intermediate buffer allocation.
- `LinkedHashMap` pre-sized with exact load factor `(fields + 1) / 0.75 + 1` — eliminates rehash on record decode.
- `StringBuilder` initial capacity = `max(1024, rawBytes.length * 3)` — avoids resize for typical payloads.
- `requireAvailable(n)` validates remaining bytes before every allocation — corrupted payloads throw `IllegalArgumentException` rather than `OutOfMemoryError`.

### Consumer Hot Path (SdiaKafkaConsumer)
- Final fields for all frequently accessed endpoint parameters — avoids virtual dispatch on every `poll()`.
- Pre-allocated `pollSuccessOffsets` map (capacity 4) — cleared and reused across poll batches, no per-poll allocation.
- Pre-allocated `payloadHexPreview` StringBuilder (64 chars) — reused per record.
- On-Premise mode: `fetch.max.wait.ms` capped at 1 s to prevent SOCKS5 idle-close from looking like a hang.
- `listTopics()` admin probe uses a 4 s timeout at startup — fails fast before Kafka's default 60 s metadata timeout.

### OSGi Bundle
- `kafka-clients` classes unpacked directly into `target/classes` via `maven-dependency-plugin:unpack` — no nested JAR, no OSGi classloader fragmentation.
- `KerberosError.class` excluded from unpack and replaced by a local stub — prevents GSSAPI resolution that CPI OSGi does not support.
- `Private-Package` embeds `org.apache.kafka.*` and JGSS stubs — no Kafka classes leak into the OSGi export graph.
- `Eclipse-BuddyPolicy: global` — allows Camel classloader to see embedded Kafka classes.
