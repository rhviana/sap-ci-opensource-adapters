# Roadmap

## v2.0.0 — Current release
- Consumer-only. SASL PLAIN · SCRAM-256 · SCRAM-512. TLS. Avro. CC tunnel.
- 498,516 messages validated. 800,000 in progress.

## v2.1.0 — In progress
- [ ] 800,000 message stress test — memory leak, JVM heap, node resilience
- [ ] Poison Pill — complete validation with Confluent Cloud
- [ ] Silent Commit — complete validation
- [ ] Earliest / Latest / None offset reset — complete validation
- [ ] Seek by Timestamp — complete validation
- [ ] Code review and extreme performance hardening

## v2.2.0 — Planned
- [ ] Producer release
- [ ] Schema Registry — subject-based lookup (not only schema ID)
- [ ] Dead Letter Queue (DLQ) routing on Poison Pill

## v3.0.0 — Future
- [ ] mTLS re-evaluation
- [ ] OAuth 2.0 re-evaluation
- [ ] Multi-partition parallel consumer
