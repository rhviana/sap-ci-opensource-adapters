# Contributing

© 2026 Ricardo Luz Holanda Viana — Integration Architect Independent Solo Researcher. Creator of DEIP - SDIA - GDCR - DDCR - ODCP - EDCP - DDCP.


Contributions are welcome. Please read this document before opening a pull request.

---

## What to contribute

- Bug fixes with a clear reproduction case
- Performance improvements with a measurable and explained justification
- Documentation improvements
- Security improvements (see [SECURITY.md](SECURITY.md) for the disclosure process)

Feature additions that change the Kafka consumer lifecycle logic (commit, seek, offset reset, poison pill, recovery) require a detailed explanation of the correctness argument — these paths are subtle and have production deployments depending on them.

## What not to contribute

- Changes that add logging to the poll loop, record processing loop, or commit path
- Changes that add external library dependencies to the OSGi bundle
- Changes that require Java > 8 at the compile target (the ADK runtime is Java 8)
- Code that depends on reflection at runtime

## Code standards

- Target: Java 8 (`<release>8</release>` in the Maven compiler plugin)
- Runtime dependencies must stay explicit and reviewable: `kafka-clients`, Avro, Camel/SAP ADK/CPI APIs, SLF4J, and OSGi bundle tooling
- No `String.intern()`, `synchronized` on `String` objects, or `new Thread()` in hot paths
- Sensitive values (tokens, passwords, keys) must not appear in any log statement
- Every allocation in the message processing hot path needs a justification comment

## Build and test

```bash
mvn clean install
```

For local Kafka testing, use the provided `docker-compose.yml` or:

```bash
docker run -p 9092:9092 apache/kafka:4.1.2
```

Deploy the produced `.esa` to a SAP Integration Suite tenant for integration testing.

## Pull request checklist

- [ ] `mvn clean install` passes
- [ ] No new log statements in the poll loop, record loop, or commit path
- [ ] No new external dependencies
- [ ] CHANGELOG.md updated
- [ ] SECURITY.md updated if the PR touches credential, token, or key handling
