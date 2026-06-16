# UI — Adapter Channel Configuration

## Connection Tab

### Kafka Cluster Bootstrap Hosts
Table with one row per broker. Each row has:
- **Host** — broker hostname or IP. No protocol prefix. Example: `sdia-kafka`, `pkc-xxxxx.us-east-1.aws.confluent.cloud`
- **Port** — String field. Supports CPI external parameter binding. Default: `9092`

### Connectivity
| Field | Values | Notes |
|---|---|---|
| Proxy Type | `Internet` · `On-Premise` | `On-Premise` activates the SAP CC TCP/SOCKS5 tunnel |
| Location ID | string | Required only when CC subaccount uses a Location ID. Leave empty otherwise. |

### Authentication Details
| Field | Values | Notes |
|---|---|---|
| Authentication | `🌐 NONE \| Plain Text` · `🔐 SASL — PLAIN \| SCRAM-256 \| SCRAM-512` | |
| Connect with TLS | `true` · `false` | Visible only when Authentication = SASL |
| SASL Mechanism | `PLAIN` · `SCRAM-SHA-256` · `SCRAM-SHA-512` | Visible only when Authentication = SASL |
| Credential Alias | string | CPI Security Material — User Credentials. Visible only when Authentication = SASL |
| Broker CA Source | `JVM Trust Store` · `Custom` | Custom requires a CA cert alias in Security Material |
| Certificate Alias | string | Visible only when Broker CA Source = Custom |

---

## Processing Tab

### Processing Details
| Field | Values | Notes |
|---|---|---|
| Topic | string | Exact name · comma-separated list · wildcard pattern |
| Group ID | string | Leave empty for auto-generated stable ID based on host+topic hash |
| Partitions | string | Optional. Comma-separated. Leave empty for Kafka group management. |
| Parallel Consumers | integer | Capped by topic partition count |
| Auto Offset Reset | `earliest` · `latest` · `none` | Applied only when group has no committed offset |
| Error Handling | `Skip` · `Stop on Error` · `Retry Failed Message` | |
| Retry Attempts | `5` · `10` | Visible only when Error Handling = Retry Failed Message |

### Fetch Details
| Field | Values | Notes |
|---|---|---|
| Min Fetch Size (MB) | integer | `0` = immediate poll |
| Max Fetch Size (MB) | `5` · `10` · `20 MB MAX` | Must exceed largest expected record |

### Avro Conversion
| Field | Values | Notes |
|---|---|---|
| Conversion Format | `None` · `JSON` · `XML` | `None` = raw bytes pass-through |
| Schema ID Source | `Magic Byte` · `Fixed Schema ID` | Magic Byte reads schema ID from bytes 1–4 of Confluent wire format |
| Fixed Schema ID | integer | Visible only when Schema ID Source = Fixed Schema ID |
| Schema Registry URL | string | Base URL. Example: `https://psrc-xxxxx.us-east-2.aws.confluent.cloud` |
| Schema Registry Credential Alias | string | CPI Security Material alias for Schema Registry API key/secret |
| Max Payload Conversion Size | `📦 1 MB (Recommended)` · `💿 2 MB (MAX)` | Records exceeding this limit are rejected at conversion |
| Max Schema Size | `🤏 5 KB (Recommended)` · `⚖️ 10 KB` · `💾 15 KB (MAX)` | Schema Registry response size limit |

---

## Error Messages

| Error Code | Cause | Fix |
|---|---|---|
| `Event.Smart.Kafka.Config.Broker.Connection.Failed` | Broker unreachable or metadata timeout | Check bootstrap host/port · CC mapping · advertised.listeners |
| `Event.Smart.Kafka.Config.CloudConnector.Tunnel.Failed` | SOCKS5 probe failed | Check CC virtual mapping · Location ID · broker reachability |
| `Target Cluster Topic footprint not found` | Topic does not exist | Create topic in broker |
| `Authentication failed` | Wrong credentials | Check CPI Security Material alias · SCRAM user registration on broker |
