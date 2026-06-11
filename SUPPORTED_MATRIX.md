# EventSmartKafka Phase 1 Support Matrix

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


Status: hardened release candidate. Not SAP-certified.

## Connection modes

| Mode | Consumer | Producer | Status |
|---|---:|---:|---|
| Cloud / Internet | Yes | Yes | Supported |
| On-Premise via SAP Cloud Connector | Yes | Yes | Supported |

## Authentication and transport

| Authentication | TLS | Private CA alias | Kafka `security.protocol` | Broker credential lookup | Status |
|---|---:|---:|---|---:|---|
| NONE | false | no | `PLAINTEXT` | no | Supported |
| NONE | true | no | `SSL` | no | Supported |
| NONE | true | yes | `SSL` + PEM trust material | no | Supported |
| SASL/PLAIN | false | no | `SASL_PLAINTEXT` | yes | Supported compatibility mode |
| SASL/PLAIN | true | no | `SASL_SSL` | yes | Supported |
| SASL/PLAIN | true | yes | `SASL_SSL` + PEM trust material | yes | Supported |
| SASL/SCRAM-SHA-256 | false | no | `SASL_PLAINTEXT` | yes | Supported compatibility mode |
| SASL/SCRAM-SHA-256 | true | optional | `SASL_SSL` | yes | Supported |
| SASL/SCRAM-SHA-512 | false | no | `SASL_PLAINTEXT` | yes | Supported compatibility mode |
| SASL/SCRAM-SHA-512 | true | optional | `SASL_SSL` | yes | Supported |
| OAuth/OAUTHBEARER | n/a | n/a | n/a | n/a | Blocked in Phase 1 |
| mTLS/client certificate | n/a | n/a | n/a | n/a | Blocked in Phase 1 |
| Kerberos/GSSAPI | n/a | n/a | n/a | n/a | Blocked by design |

## Enforcement

- Metadata exposes only `None` and `SASL`.
- `credentialAlias` appears only for `SASL`.
- `connectWithTls` is independent of authentication and is visible for `None` and `SASL`.
- Private CA alias is server-side TLS trust only. It is not mTLS.
- Runtime validation rejects stale or manually injected OAuth, mTLS, Kerberos/GSSAPI and unknown auth profiles.
