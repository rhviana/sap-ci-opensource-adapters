# Security

## Authentication Profiles (v2.0)

| Profile | Protocol | Credential Required |
|---|---|---|
| NONE / Plain Text | PLAINTEXT | No |
| SASL — PLAIN | SASL_PLAINTEXT or SASL_SSL | Yes — User Credentials alias |
| SASL — SCRAM-SHA-256 | SASL_PLAINTEXT or SASL_SSL | Yes — User Credentials alias |
| SASL — SCRAM-SHA-512 | SASL_PLAINTEXT or SASL_SSL | Yes — User Credentials alias |

**mTLS and OAuth 2.0 are not supported in v2.0.** They will be re-evaluated for a future release.

---

## SAP Cloud Connector SOCKS5 Tunnel

The adapter implements the SAP Connectivity proprietary SOCKS5 extension:

- **Method 0x80** — SAP JWT bearer token auth. Token is stored as `byte[]` and zeroed immediately after the SOCKS5 handshake write. Never stored as `String`.
- **Method 0x02** — RFC 1929 username/password fallback.
- **Method 0x00** — No-auth (local/test topologies only).

The SOCKS5 greeting offers all three methods in priority order. SAP CC selects the strongest available. If the CC rejects all methods (`0xFF`), startup fails immediately with a clear error.

SOCKS5 handshake uses a 15-second timeout. Data pipes run without timeout — Kafka connections can be idle for extended periods without triggering a false disconnect.

Location ID is Base64-encoded before transmission and limited to 255 bytes encoded length.

---

## Credential Handling

- Credentials are resolved from SAP CPI Secure Store via `SdiaKafkaCredentialsResolver` on every deploy — never cached across iFlow restarts.
- For NONE / PLAINTEXT, no credential lookup is performed — prevents "alias not found" errors and eliminates the metadata timeout that occurs when a SASL lookup is accidentally triggered on a plain-text broker.
- JAAS config strings are built in-memory with `StringBuilder` and escape `\` and `"` inline — no intermediate `String` objects holding passwords.

---

## TLS

When `Connect with TLS = ON`, the adapter sets `security.protocol = SASL_SSL`. SSL material is applied by `SdiaKafkaSslConfigurator`:

- **JVM Trust Store** (default) — uses the CPI JVM cacerts. Suitable for public CAs (Confluent Cloud, Aiven, etc.).
- **Custom** — a CA certificate alias from CPI Security Material is loaded as a custom truststore. Required for self-signed or private CA certificates (on-premise brokers).

`ssl.endpoint.identification.algorithm` follows the Kafka default (`https`) unless explicitly overridden. For self-signed certs with `CN=127.0.0.1`, set it to empty string in the producer/consumer client config.

---

## OSGi Isolation

- Kerberos/GSSAPI classes (`KerberosError`, `KerberosLogin`, `KerberosClientCallbackHandler`) are replaced by no-op stubs. CPI OSGi does not export `org.ietf.jgss` and GSSAPI is not used.
- JGSS stubs (`GSSContext`, `GSSCredential`, etc.) are embedded in the bundle's `Private-Package` — they prevent `ClassNotFoundException` in Kafka's optional Kerberos code paths without requiring the actual GSSAPI implementation.
- `DynamicImport-Package: *` is set as a safety net for optional Kafka compression codecs (snappy, lz4, zstd) that may or may not be present in the CPI OSGi container.
