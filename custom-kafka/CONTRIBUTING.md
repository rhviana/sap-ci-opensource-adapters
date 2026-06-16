# Contributing

This project is maintained by Ricardo Luz Holanda Viana. Contributions are welcome via pull request.

## Requirements

- Java 8
- Maven 3.6+
- No local SAP JARs needed — all dependencies from Maven Central

## Build

```bash
mvn clean install
```

## Guidelines

- Do not remove or alter the copyright header in any source file.
- All changes to `metadata.xml` must be reflected in all three copies: `src/main/resources/META-INF/metadata.xml`, `src/main/resources/metadata.xml`, `src/main/resources/metadata/metadata.xml`.
- The TCP tunnel (`SdiaCloudConnectorTcpTunnel`) is production-validated with 500k+ messages. Changes require full regression testing.
- The Avro decoder (`SdiaKafkaAvroConverter`) has zero external dependencies by design. Do not add Avro library dependencies.
- `kafkaPortRow` is `xsd:string` in metadata and `String` in `SdiaKafkaEndpoint`. Do not revert to `xsd:integer`.
- Producer code will be added in a future release. Do not add producer logic to the consumer classes.

## License

By contributing, you agree your contributions are licensed under Apache 2.0 and MIT (dual-license).
