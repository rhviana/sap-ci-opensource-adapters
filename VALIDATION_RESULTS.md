# EventSmartKafka Phase 1 Validation Results Template

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


Use this file to record tenant-level evidence. The source package enforces the matrix below; real CPI/Kafka evidence must be filled by the maintainer after deployment tests.

| Scenario | Cloud Consumer | Cloud Producer | OnPrem Consumer | OnPrem Producer | Expected result |
|---|---:|---:|---:|---:|---|
| NONE + PLAINTEXT | PASS | PASS | PASS | PASS | No broker credential lookup; `security.protocol=PLAINTEXT` |
| NONE + TLS public CA | PASS | PASS | PASS | PASS | No broker credential lookup; `security.protocol=SSL` |
| NONE + TLS private CA | PASS | PASS | PASS | PASS | No broker credential lookup; `security.protocol=SSL`; private CA from CPI Keystore |
| SASL/PLAIN + PLAINTEXT | PASS | PASS | PASS | PASS | Credential lookup required; `security.protocol=SASL_PLAINTEXT` |
| SASL/PLAIN + TLS public CA | PASS | PASS | PASS | PASS | Credential lookup required; `security.protocol=SASL_SSL` |
| SASL/PLAIN + TLS private CA | PASS | PASS | PASS | PASS | Credential lookup required; private CA from CPI Keystore |
| SCRAM-SHA-256 + PLAINTEXT | PASS | PASS | PASS | PASS | Credential lookup required; `security.protocol=SASL_PLAINTEXT` |
| SCRAM-SHA-256 + TLS | PASS | PASS | PASS | PASS | Credential lookup required; `security.protocol=SASL_SSL` |
| SCRAM-SHA-512 + PLAINTEXT | PASS | PASS | PASS | PASS | Credential lookup required; `security.protocol=SASL_PLAINTEXT` |
| SCRAM-SHA-512 + TLS | PASS | PASS | PASS | PASS | Credential lookup required; `security.protocol=SASL_SSL` |
| OAuth/OAUTHBEARER | EXPECTED FAIL | EXPECTED FAIL | EXPECTED FAIL | EXPECTED FAIL | Blocked in Phase 1 |
| mTLS/client certificate | EXPECTED FAIL | EXPECTED FAIL | EXPECTED FAIL | EXPECTED FAIL | Blocked in Phase 1 |
| Kerberos/GSSAPI | EXPECTED FAIL | EXPECTED FAIL | EXPECTED FAIL | EXPECTED FAIL | Blocked by design |

Evidence to attach before calling a release production-ready: screenshots/log excerpts from CPI tenant, sanitized Kafka broker config, Cloud Connector mapping, throughput/p95/p99 numbers, heap/thread snapshots, and failure-mode logs showing no secret/payload leakage.
