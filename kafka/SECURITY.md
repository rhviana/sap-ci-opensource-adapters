# Security

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


## Reporting a vulnerability

Please do **not** open a public GitHub issue for security vulnerabilities.

Send a private report to the maintainer via GitHub's **Security Advisories** feature (see the **Security** tab in this repository → **Report a vulnerability**). Include a description of the issue, reproduction steps, and any relevant logs or code references. You will receive a response within 72 hours.

---

## Security architecture

### Bearer JWT handling

The SAP Connectivity bearer JWT (required for SOCKS5 method `0x80` authentication against the Cloud Connector) is handled as follows:

- Converted into `byte[]` in `ProxyConfig` for relay use. Per-handshake byte-array copies are zeroed immediately after each SOCKS5 write using `Arrays.fill(bytes, (byte) 0)`. The original token `String` comes from the SAP Connectivity API and remains under runtime ownership until GC.
- Never logged. The `maskedAuthority()` method returns only `host:port`. No log statement prints the token or any prefix of it.
- Never passed to `String.intern()`. Credential alias locking avoids interned strings and does not pin credential material in the JVM shared string pool.
- The source `String` supplied to the `ProxyConfig` constructor is converted to `byte[]` once at construction time and is subsequently eligible for normal GC.

### SAP CPI credentials

`SdiaKafkaCredentialsResolver` reads credentials from the SAP CPI Secure Store via `SecureStoreService`. The password `char[]` returned by `UserCredential.getPassword()` is zeroed with `Arrays.fill(chars, ' ')` immediately after the Kafka-required `String` is constructed. The resulting `String` remains in memory while Kafka client properties use it. The resolver caches resolved `SdiaKafkaCredentials` objects for the lifetime of the adapter deployment and clears the cache on `doStart()` (redeploy / iFlow restart).

### Unsupported authentication modes in Phase 1

The adapter fails closed for OAuth/OAUTHBEARER, mTLS/client certificate, Kerberos/GSSAPI and unknown authentication profiles. These options are not displayed in the metadata and are rejected if injected manually through URI parameters or stale iFlow configuration.

### SOCKS5 authentication negotiation

The adapter offers authentication methods in this order:

1. `0x80` — SAP proprietary JWT method (offered when a bearer token is available)
2. `0x02` — RFC 1929 username/password fallback
3. `0x00` — disabled by default. It is offered only when `-Deventsmart.kafka.cc.allowNoAuthFallback=true` is explicitly set for a controlled local/test topology.

If the proxy returns `0xFF` (no acceptable method), the connection is immediately refused with a clear exception. Method `0x80` is the expected method for SAP BTP Connectivity in production. If no bearer token is available, the relay fails closed unless the explicit test-only fallback property above is enabled.

### TLS and broker authentication — Phase 1

- Authentication NONE never resolves a broker credential alias and never injects `sasl.*` properties. With TLS disabled it produces `security.protocol=PLAINTEXT`; with TLS enabled it produces `security.protocol=SSL`.
- Authentication SASL is the only mode that resolves SAP CPI User Credentials and injects JAAS. With TLS disabled it produces `security.protocol=SASL_PLAINTEXT`; with TLS enabled it produces `security.protocol=SASL_SSL`.
- Broker CA certificates are loaded from the SAP CPI Keystore Service as PEM-encoded material and passed to the Kafka client via `ssl.truststore.type=PEM` and `ssl.truststore.certificates`. No filesystem trust store is written.
- TLS with public CA uses the platform/JVM trust store and does not require a certificate alias.
- mTLS/client-certificate authentication is intentionally blocked in Phase 1. Any configured `mtlsKeyAlias` or `Client Certificate` authentication value fails closed during validation.

### Payload limits

| Mode | Limit | Enforcement |
|---|---|---|
| Avro conversion | 1–5 MB (configurable) | Before Schema Registry contact |
| Raw pass-through | 20 MB (hard) | Before `exchange.getIn().setBody()` |
| Schema JSON | 1–100 KB (configurable) | After HTTP response, before cache |

These limits protect the CPI worker from OOM conditions caused by unexpectedly large Kafka messages or malformed Schema Registry responses.

### Schema Registry

HTTPS is enforced by default for non-loopback Schema Registry URLs. HTTP is accepted only for loopback development endpoints or when `-Deventsmart.kafka.schemaRegistry.allowInsecureHttp=true` is explicitly set. Error diagnostics redact registry body content and include only size/hash metadata.


- HTTP connections use `conn.setUseCaches(false)` to prevent stale responses from the JVM HTTP cache.
- The schema JSON size is validated against the configured limit **before** the schema is stored in cache.
- Schema IDs are integers; the adapter validates that they are positive before any registry call.

### Logging

The adapter avoids deliberate secret and payload-body logging:

- Bearer token: masked to `host:port` in all log statements
- Location ID: masked to `X***Y` (first and last character only)
- Schema Registry credentials: only the alias name is logged, never the username or password
- Kafka JAAS config: never logged; the `sasl.jaas.config` property contains cleartext credentials

### Thread safety

- `SdiaKafkaCredentialsResolver.CACHE` — `ConcurrentHashMap` with `computeIfAbsent`
- `SdiaSchemaRegistryClient.CACHE` — `LinkedHashMap` (LRU) guarded by `CACHE_LOCK`
- `SdiaCloudConnectorTcpTunnel.running` — `AtomicBoolean`
- `TunnelServer.workerPool` — bounded `ThreadPoolExecutor` (max 32 workers per port)

---

## Known limitations

- The SOCKS5 relay binds to `127.0.0.1:<brokerPort>`. Passive reuse is allowed only for relays already owned by the same JVM/runtime registry. If the port is occupied by an unknown process, startup fails closed with `BindException`.
- The bearer JWT used for SOCKS5 auth is a short-lived SAP Connectivity token retrieved from `CloudConnectorProperties.getAdditionalHeaders()`. Token refresh is handled by the SAP Connectivity service; the adapter does not implement token refresh logic.
- Schema cache TTL is 1 hour. The cache key includes registry URL, schema ID, and a fingerprint of the Schema Registry principal to avoid cross-principal reuse. If a non-Confluent registry mutates an existing ID, the old schema is served until TTL expiry or redeploy.
