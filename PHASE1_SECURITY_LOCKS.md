# Phase 1 Security Locks

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


This release intentionally narrows the security surface. The adapter does not try to expose every Kafka authentication mode.

## Hard rules

1. Broker credentials are resolved only when `authentication=SASL`.
2. `authentication=None` never resolves `credentialAlias`, even in On-Premise mode.
3. TLS is controlled only by `connectWithTls`.
4. Private CA support means server certificate trust only; it is not mTLS.
5. OAuth/OAUTHBEARER is blocked by metadata and runtime validation.
6. mTLS/client certificate is blocked by metadata and runtime validation.
7. Kerberos/GSSAPI is blocked by metadata and runtime validation.

## Property generation

| Input | Generated Kafka properties |
|---|---|
| `authentication=None`, `connectWithTls=false` | `security.protocol=PLAINTEXT`; no `sasl.*`; no credential lookup |
| `authentication=None`, `connectWithTls=true` | `security.protocol=SSL`; optional PEM trust material; no `sasl.*`; no credential lookup |
| `authentication=SASL`, `connectWithTls=false` | `security.protocol=SASL_PLAINTEXT`; `sasl.mechanism`; JAAS from CPI User Credentials |
| `authentication=SASL`, `connectWithTls=true` | `security.protocol=SASL_SSL`; `sasl.mechanism`; JAAS from CPI User Credentials; optional PEM trust material |

## On-Premise behavior

On-Premise mode affects networking only. It opens the Cloud Connector TCP relay and rewrites the Kafka bootstrap address to the local relay. It must not change broker authentication semantics. Therefore NONE+PLAINTEXT+On-Premise starts without `credentialAlias`.
