# POM tuning decision — EventSmartKafka v2.0.1

## Decision

Do not upgrade the adapter runtime to Apache Camel 4.4 or Kafka clients 4.x for the CPI Java 8 release line.

## Why

- Apache Camel is provided by the SAP CPI runtime. The adapter imports Camel `[3.14,4)`, so Camel 4.x is intentionally outside the allowed range.
- Camel 4.4 requires Java 17/21.
- Kafka 4.x removed Java 8 support. Kafka 4.x clients require Java 11+, while the adapter must remain Java 8 bytecode/runtime safe for CPI OSGi.
- The production-validated adapter baseline remains `kafka-clients 3.6.2`.

## What changed in the POM

- Added explicit comments blocking Camel 4.x / Kafka 4.x for this CPI line.
- Added `bundle.version` property so experimental bundles can change OSGi version and avoid CPI cache confusion.
- Added Java/Maven build guardrails through Maven Enforcer.
- Kept Java 8 bytecode via `maven.compiler.release=8`.
- Added unpack exclusions for `module-info.class`, `META-INF/versions/**`, and Maven metadata.
- Added optional profile:

```bash
mvn clean install -Pkafka-3.9.1-experimental
```

This profile changes only:

```xml
<kafka.clients.version>3.9.1</kafka.clients.version>
<bundle.version>3.0.0.kafka391</bundle.version>
```

## Required validation before promoting Kafka 3.9.1 to default

1. Import ESA into CPI without OSGi resolution errors.
2. Validate 10x smoke across all iFlows.
3. Validate 100k conservative smoke.
4. Validate 800k stress once.
5. Validate Cloud Connector TCP/SOCKS5 on 19091, 19092 and 19093.
6. Validate Avro Magic Byte and Fixed Schema ID.
7. Validate Schema Registry GZIP sniff.
8. Validate poison pill skip/commit behavior.
9. Validate TIMESEEK and single-message isolation.
10. Validate no new thread/socket leaks after redeploy.

## Release recommendation

Ship v2.0.1 with Kafka 3.6.2 as the stable default.
Treat Kafka 3.9.1 as a controlled compatibility experiment, not a last-minute release upgrade.
