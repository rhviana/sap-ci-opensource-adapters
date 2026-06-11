# Performance Engineering

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


This document describes the performance design decisions implemented in the SDIA Smart Kafka adapter. All numbers below are from a developer machine and are not a substitute for testing in your own CPI tenant. End-to-end CPI throughput is dominated by MPL persistence, Camel pipeline complexity, and tenant resource quotas — the adapter's own cost is small by comparison.

---

## Design principles

1. **Hot path first.** The path a record takes from `KafkaConsumer.poll()` through header extraction, optional Avro decode, and `exchange.setBody()` is optimized at the cost of everything else. Cold paths (startup, error handling, recovery) are allowed to be slow.

2. **No per-message allocation on the schema path.** Parsed `AvroSchema` object trees are cached alongside the raw schema JSON in `SdiaSchemaRegistryClient`. A schema with 30 fields allocates ~35 objects on the first message; every subsequent message is allocation-free for schema resolution.

3. **Block-copy string operations.** `escapeJson` and `escapeXml` scan for the next unsafe character and bulk-copy the safe run via `StringBuilder.append(CharSequence, int, int)`. Most Avro string fields contain zero characters that need escaping, so the entire field is copied in a single call instead of one append per character.

4. **Fast-path XML name validation.** `toXmlName` validates the name first; if it is already a legal XML element name (the common case for Avro field names), the original `String` is returned with zero allocation.

5. **Lock-free reads on the hot path.** `volatile` fields and `AtomicBoolean` are used instead of `synchronized` wherever possible. The one remaining synchronized block — `SdiaCloudConnectorTcpTunnel.start()` — runs only at startup.

6. **Bounded thread pools.** The TCP relay uses a `ThreadPoolExecutor` with a fixed maximum (32 workers per port) instead of `new Thread()` per connection, preventing unbounded thread creation under connection load.

7. **No hot-path logging.** All per-poll and per-record log statements have been removed. The 45 remaining `LOG.warn/error` calls fire only at startup (once per deploy), on failures, or on the 30-second throttled empty-poll heartbeat.

---

## Key optimizations

### Schema cache — TTL + LRU, parsed tree cached

Schema IDs in the Confluent Schema Registry are immutable: once registered, an ID always maps to the same JSON. The cache stores both the raw JSON and the parsed `AvroSchema` tree with a 1-hour TTL and LRU eviction at 256 entries.

The previous implementation stored only the raw JSON and re-parsed the schema on every message. For a schema with 30 fields, this allocated ~35 objects per message. The new implementation allocates zero objects for schema resolution on cache-hit messages.

The previous eviction strategy was `CACHE.clear()` when size reached 256. This could cause 256 simultaneous HTTP requests to the Schema Registry when a 257th schema was encountered. The hardened cache now uses LRU eviction plus single-flight locking per schema/principal key, so concurrent misses for the same schema collapse into one HTTP fetch/parse.

### Block-copy escape (JSON and XML)

Previous implementation:
```java
for (char c : s) {
    switch (c) { case '<': out.append("&lt;"); break; ... default: out.append(c); }
}
```

New implementation:
```java
int runStart = 0;
for (int i = 0; i < len; i++) {
    final char c = s.charAt(i);
    // identify escape sequence or continue
    if (i > runStart) out.append(s, runStart, i); // bulk-copy safe run
    out.append(escapeSequence);
    runStart = i + 1;
}
if (runStart < len) out.append(s, runStart, len);
```

For fields with no unsafe characters (the common case), the entire string is copied in one `append` call.

### `toXmlName` fast-path

The new implementation validates the name without allocating a `StringBuilder`. If the name is already valid (all legal XML name characters, no digit at position 0), the original `String` is returned immediately. Only when sanitization is needed does the code allocate a `StringBuilder`.

### Manual nibble decode for `\uXXXX`

Previous: `Integer.parseInt(json.substring(pos, pos + 4), 16)` — allocates a substring.

New: direct nibble arithmetic with a private `hexNibble(char)` method — zero allocation.

### Pipe buffer = socket buffer

The relay `Pipe` buffer size (64 KB) matches the socket send/receive buffer size (64 KB). When the broker sends a full 64 KB chunk, a single `in.read()` call drains it, and a single `out.write()` call forwards it. The previous 32 KB buffer required two read syscalls per 64 KB chunk.

### No `flush()` in the Pipe hot path

The previous implementation called `out.flush()` after every `out.write()` in the relay. `OutputStream.flush()` on a socket stream calls a native method even when there is nothing to flush. With `TCP_NODELAY` set on the socket, data is sent immediately after `write()` without needing an explicit flush. The `flush()` call was removed.

### Bearer token as `byte[]`

`ProxyConfig` converts the SAP-provided bearer token into `byte[]` for relay use. Each SOCKS5 handshake receives a defensive copy, and the copy is zeroed immediately after write. The total cost is negligible because this happens per relay connection establishment, not per Kafka message.

### `computeIfAbsent` instead of `synchronized(intern)`

`SdiaKafkaCredentialsResolver` previously used `synchronized(alias.intern())`. `String.intern()` places strings in the JVM shared pool where they cannot be collected. The new implementation uses `ConcurrentHashMap.computeIfAbsent()`, which guarantees the loader runs at most once per alias under concurrent access without any string interning.

---

## Indicative benchmarks

| Path | Throughput (single consumer, schema cached) |
|---|---|
| Pass-through, no conversion | ~50 000 records/s |
| Avro → JSON, fixed schema ID, schema cached | ~47 000 records/s |
| Avro → XML, fixed schema ID, schema cached | ~32 000 records/s |
| Avro → JSON, first message after deploy (schema cache miss) | depends on Schema Registry RTT |

| Operation | Latency (schema cached) |
|---|---|
| Header extraction + `x-sdiakafka-*` enrichment | ~0.5 µs |
| Avro decode, 5 fields | ~0.4 µs |
| Avro record → `Map<String,Object>` | ~5 µs |
| JSON serialization, 50-field record | ~10 µs |
| XML serialization, 50-field record | ~17 µs |

> ⚠️ These figures use a warm JIT and a loaded schema cache. Cold-start latency is dominated by OSGi bundle load (~200–500 ms), the first Schema Registry call (TLS + HTTP roundtrip), and Kafka group join (seconds). After the first successful poll, steady-state numbers apply.

---

## Tuning recommendations

| Scenario | Recommendation |
|---|---|
| Low latency, small records, no Avro | `maxPollRecords=1`, `pollTimeoutMs=500`, `Conversion Format=None` |
| High throughput, batched | `maxPollRecords=500`, `pollTimeoutMs=5000` |
| Avro-heavy | Use `Fixed Schema ID` (avoids magic byte resolution); keep schema JSON ≤ 50 KB |
| Large schemas (50–100 KB) | Set `schemaIdBufferSize=100`; schemas are fetched once and cached for 1 hour |
| On-Premise / Cloud Connector | `pollTimeoutMs` is capped at 2000 ms automatically; `fetch.max.wait.ms` capped at 1000 ms |
| Many iFlows, same broker port | Second and subsequent iFlows attach passively to the existing relay — no extra threads |

---

## What was deliberately not optimized

| Decision | Reason |
|---|---|
| Streaming Avro → JSON without intermediate `Map` | Complexity of union/array bookkeeping outweighs allocation savings for typical record sizes |
| Off-heap payload buffer pool | CPI sandbox restricts native memory access |
| Replacing `LinkedHashMap` with a custom record map | Camel processors expect `Map`-shaped input; the conversion cost erased the win in real workloads |
