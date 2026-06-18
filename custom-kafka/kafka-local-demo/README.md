# Kafka Local Demo

Local Apache Kafka validation environment for EventSmartKafka.

This folder contains Docker Compose setup, demo users, certificates, client properties, Avro payloads, smoke tests and stress scripts used to validate EventSmartKafka with SAP Cloud Connector TCP/SOCKS5.

## Structure

| Path | Purpose |
|---|---|
| setup/ | Kafka startup and topic creation scripts. |
| tests/smoke/ | Small validation tests. |
| tests/stress/ | Conservative stress test scripts. |
| tests/tools/ | Topic and consumer group inspection scripts. |
| security/users/ | Demo user documentation. |
| security/certificates/ | Local demo certificates. |
| config/client-properties/ | Kafka CLI client properties. |
| payloads/avro/schemas/ | Avro schema files. |
| payloads/avro/binary/ | Pre-generated Avro binary payloads. |
| scripts/generators/ | Payload generation scripts. |

## Demo Credentials

User: sdia  
Password: SDIABRASIL

These credentials are for local demo only.

## License

Apache License 2.0.
