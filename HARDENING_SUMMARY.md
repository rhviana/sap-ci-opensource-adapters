# EventSmartKafka hardening summary

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


This archive contains the hardened EventSmartKafka adapter source and a source-validation class jar.

## Main changes

- Adapter identity changed to `EventSmartKafka` / vendor `rhviana` / version `1.0.0`.
- Maven coordinates changed to `consulting.rhviana.eventsmart:event-smart-kafka-adapter:1.0.0-SNAPSHOT`.
- Kafka client version raised to `3.9.2`.
- Avro dependency added explicitly as `org.apache.avro:avro:1.11.4`.
- Dangerous local stubs for `javax.security.auth.kerberos`, `org.ietf.jgss`, and Kafka Kerberos packages removed.
- Kerberos/GSSAPI is explicitly rejected at configuration time.
- Producer now uses the same bootstrap resolution path as Consumer and supports the bootstrap table / on-premise relay path.
- Phase 1 authentication locked to NONE and SASL only. OAuth/OAUTHBEARER and mTLS/client certificate are hidden from metadata and rejected at runtime.
- Producer and Consumer security paths now generate the same protocol matrix: NONE+PLAINTEXT, NONE+SSL, SASL_PLAINTEXT and SASL_SSL.
- Broker credentials are resolved only when Authentication = SASL. Authentication NONE never performs a Secure Store/User Credential lookup.
- Cloud Connector TCP relay hardened:
  - passive reuse only for same-JVM owned relays;
  - unknown occupied local ports fail closed;
  - bounded live connection slots;
  - shared bounded worker/pipe pools;
  - `NO_AUTH` disabled by default;
  - legacy relay code removed from `SdiaKafkaConsumer` bytecode.
- Schema Registry client hardened:
  - per-schema/principal single-flight cache miss handling;
  - cache key includes registry URL, schema ID, and principal fingerprint;
  - HTTPS enforced for non-loopback URLs by default;
  - error diagnostics redact bodies and expose only size/hash metadata.
- Error exchanges no longer contain raw Kafka payload bodies. They expose payload size and SHA-256 hash instead.
- Retry policy adjusts effective `max.poll.interval.ms` and forces `max.poll.records=1` for in-poll retry mode.
- Metadata XML defaults aligned with runtime defaults and unsupported PROTOBUF option removed.
- Runtime status registration implemented instead of no-op.
- README/SECURITY/PERFORMANCE claims corrected to avoid overstating guarantees not validated in a real CPI/Kafka environment.

## Validation performed here

- `javac --release 8` source compilation succeeded.
- The three metadata XML files are well-formed.
- No real SAP CPI, Kafka, Schema Registry, or Cloud Connector test was executed.
- Maven/SAP ADK archive build was not executed because `mvn` is not installed in this environment.

## Distribution note

SAP proprietary/API jars are not included in the clean archive. Put your tenant-compatible SAP compile-time jars under `compile-libs/` before running the real Maven/ADK build.
