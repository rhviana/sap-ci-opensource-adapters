/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 * SAP BTP Integration Suite Expert | Developer | SAP Press Author
 * Enterprise Messaging (SAP Press, 2021)
 * Creator of DEIP · SDIA · GDCR · DDCR · ODCP · EDCP · DDCP
 * ORCID: 0009-0009-9549-5862
 *
 * ----------------------------------------------------------------------------
 * Dual-Licensed under:
 *
 * Apache License, Version 2.0
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * MIT License
 * https://opensource.org/licenses/MIT
 *
 * You may use this software under either license at your option.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under these licenses is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * ----------------------------------------------------------------------------
 *
 * ⚠️  NOTICE — This header must NOT be removed or altered in any distribution,
 * derivative work, or reuse of this source code, in whole or in part.
 * Removal of this header constitutes a violation of the license terms
 * and applicable intellectual property rights.
 *
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import org.apache.camel.*;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.support.DefaultPollingEndpoint;

/**
 * SDIA Kafka custom adapter endpoint.
 * Metadata resolution for SAP ADK high-throughput runtime.
 */
@UriEndpoint(
        scheme = "EventSmartKafka",
        syntax = "EventSmartKafka://kafka",
        title  = "EventSmartKafka"
)
public class SdiaKafkaEndpoint extends DefaultPollingEndpoint {

    @UriParam
    private String firstUriPart;

    @UriParam
    private String kafkaClusterHostsTable;

    @UriParam
    private String kafkaHostRow;

    @UriParam
    private String kafkaPortRow;

    /**
     * Resolved bootstrap servers string — cached after first resolution.
     * The table value is immutable per deploy; caching avoids repeated XML/JSON scanning
     * on every poll() cycle (called every 1s per iFlow).
     */
    private transient volatile String resolvedBootstrapServersCache;

    @UriParam
    private String proxyType = "Internet";

    @UriParam
    private String sapSapccLocationId;

    @UriParam
    private String authentication = "SASL";

    @UriParam
    private Boolean connectWithTls = Boolean.TRUE;

    @UriParam
    private String saslMechanism = "PLAIN";

    @UriParam
    private String credentialAlias;

    @UriParam
    private String certificateAlias;

    @UriParam
    private String brokerCaSource = "None";

    @UriParam
    private String topicPattern;

    @UriParam
    private String partitionAssignmentMode = "Automatic Assignment";

    private transient boolean partitionAssignmentModeConfigured = false;

    @UriParam
    private String partitions;

    @UriParam
    private Integer parallelConsumers = 1;

    @UriParam
    private Integer minFetchSize = 1;      // metadata fixed profile in MB: 1, 5, 10 or 20

    @UriParam
    private Integer maxFetchSize = 20;     // metadata fixed profile in MB: 1, 5, 10 or 20

    @UriParam
    private Integer maxPartitionFetchSize = null; // legacy field; runtime derives from maxFetchSize

    @UriParam
    private Integer maxFetchWaitTime = 500; // hidden runtime default in ms

    @UriParam
    private Integer maxRetryBackoffMs = 1000;

    @UriParam
    private Long reconnectDelayS = 1L;

    @UriParam
    private Long heartbeatIntervalS = 3L;

    @UriParam
    private Long sessionTimeoutS = 10L;

    @UriParam
    private Integer maxPollIntervalMs = 60000; // internal default: avoid 5-minute zombie waits during CPI redeploy/rebalance

    @UriParam
    private String errorHandling = "Stop on Error";

    @UriParam
    private Integer retryAttempts = 5;

    @UriParam
    private String conversionFormat = "None";

    @UriParam
    private String messageConversion = "RAW_BYTES";

    @UriParam
    private String payloadEncoding = "AVRO_SCHEMA_REGISTRY_WIRE";

    @UriParam
    private String avroOutputFormat = "JSON";

    @UriParam
    private String schemaRegistryType = "CONFLUENT_KARAPACE";

    @UriParam
    private String schemaIdResolutionMode = "Magic Byte";

    @UriParam
    private String schemaRegistryHostAddress;

    @UriParam
    private String schemaRegistryCredentialAlias;

    @UriParam
    private String schemaRegistrySchemaId;

    @UriParam
    private Long schemaRegistryConnectTimeoutS = 5L;

    @UriParam
    private Long schemaRegistryReadTimeoutS = 10L;

    /*
     * Metadata field names exposed by metadata.xml.
     * Keep these exact names. ADK/Camel binds URI parameters by JavaBean name.
     * The previous runtime-only aliases (maxPayloadSizeMb/schemaIdSizeBuffer)
     * are still kept below to avoid breaking older ESA/iFlow configurations.
     */
    @UriParam
    private String maxPayloadConversionSize = "1";

    @UriParam
    private String schemaIdBufferSize = "5";

    @UriParam
    private Integer maxPayloadSizeMb = 1;

    @UriParam
    private Integer schemaIdSizeBuffer = 50;

    @UriParam
    private Long pollTimeoutS = 1L;

    @UriParam
    private Long pollTimeoutMs = 1000L;

    @UriParam
    private Long initialDelayS = 0L;

    @UriParam
    private Long delayS = 1L;

    @UriParam
    private Integer maxPollRecords = 10;

    @UriParam
    private String groupId;

    @UriParam
    private String autoOffsetReset = "earliest";

    @UriParam
    /** @deprecated OAuth removed in v2.0 — use SASL PLAIN or SCRAM. */
    @Deprecated private String oauthTokenEndpoint;

    @UriParam
    @Deprecated private String oauthClientId;

    @UriParam
    @Deprecated private String oauthClientSecretAlias;

    @UriParam
    @Deprecated private String oauthScope;

    @UriParam
    /** @deprecated mTLS removed in v2.0 — use SASL PLAIN or SCRAM. */
    @Deprecated private String mtlsKeyAlias;

    @UriParam
    private String seekToTimestamp;

    @UriParam
    private String recoveryMode = "Disabled";

    @UriParam
    private String recoveryTimestampFormat = "Auto Detect";

    @UriParam
    private String recoveryTimestampValue;

    @UriParam
    private String recoveryReadMode = "Read only first matched message";

    @UriParam
    private Long requestTimeoutS = 30L;

    @UriParam
    private Long requestTimeoutMs = 30000L;

    public SdiaKafkaEndpoint() {
        applySchedulingDefaults();
    }

    public SdiaKafkaEndpoint(final String endpointUri, final SdiaKafkaComponent component) {
        super(endpointUri, component);
        applySchedulingDefaults();
    }

    private void applySchedulingDefaults() {
        // Runtime contract for the sender adapter: poll immediately after deploy and then every second.
        // The channel no longer exposes Initial Delay / Delay because persisted UI values can make CPI look zombie.
        this.initialDelayS = 0L;
        this.delayS = 1L;
        super.setInitialDelay(0L);
        super.setDelay(1000L);
    }

    @Override
    public Producer createProducer() throws Exception {
        throw new UnsupportedOperationException(
            "[EventSmartKafka] Producer is not available in this release. Only Consumer is supported.");
    }

    @Override
    public Consumer createConsumer(final Processor processor) throws Exception {
        SdiaKafkaConsumer consumer = new SdiaKafkaConsumer(this, processor);
        configureConsumer(consumer);
        return consumer;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    /**
     * Bootstrap resolver for ADK native AttributeTableMetadata.
     * Tolerant to tag attributes (e.g. {@code <row index="0">} instead of bare {@code <row>}),
     * to JSON with arbitrary whitespace/newlines, and to single or double quoted cell ids.
     * Caches the resolved result per endpoint instance.
     */
    public String resolveBootstrapServers() {
        // Cache hit — avoid XML/JSON scanning on every poll() iteration
        final String cached = resolvedBootstrapServersCache;
        if (cached != null) return cached.isEmpty() ? null : cached;

        final String table = kafkaClusterHostsTable == null ? null : kafkaClusterHostsTable.trim();
        if (table == null || table.isEmpty()) {
            final String legacy = firstUriPart == null ? null : firstUriPart.trim();
            final String legacyResult = (legacy != null && !legacy.isEmpty() && !"foo".equalsIgnoreCase(legacy) && !"kafka".equalsIgnoreCase(legacy)) ? legacy : null;
            resolvedBootstrapServersCache = legacyResult == null ? "" : legacyResult;
            return legacyResult;
        }

        // 1. XML path — tolerant to <row>, <row index="0">, <row id="0">, any attributes.
        //    We search for "<row" (no closing angle bracket) so attributes never break the scan.
        if (table.indexOf("<row") >= 0 || table.indexOf("<cell") >= 0) {
            final StringBuilder result = new StringBuilder(256);
            int rowIdx = 0;
            while ((rowIdx = table.indexOf("<row", rowIdx)) != -1) {
                final int rowTagClose = table.indexOf('>', rowIdx);
                if (rowTagClose == -1) break;
                final int endRowIdx = table.indexOf("</row>", rowTagClose);
                if (endRowIdx == -1) break;

                final String rowSegment = table.substring(rowTagClose + 1, endRowIdx);
                String host = extractXmlCellValue(rowSegment, "kafkaHostRow");
                String port = extractXmlCellValue(rowSegment, "kafkaPortRow");
                if (host == null || host.isEmpty()) {
                    // ADK fallback: some table serializers emit positional cells only:
                    // <row id="0"><cell>host</cell><cell>9092</cell></row>
                    host = extractXmlCellValueByIndex(rowSegment, 0);
                    port = extractXmlCellValueByIndex(rowSegment, 1);
                }
                if (host != null && !host.isEmpty()) {
                    if (result.length() > 0) result.append(',');
                    result.append(host).append(':').append(port != null && !port.isEmpty() ? port : "9092");
                }
                rowIdx = endRowIdx + 6;
            }
            if (result.length() > 0) {
                final String r = result.toString();
                resolvedBootstrapServersCache = r;
                return r;
            }
        }

        // 2. JSON path — tolerant to leading/trailing whitespace, newlines, and spaced punctuation.
        //    We search for the first '[' or '{' anywhere in the string instead of requiring
        //    charAt(0) to be '[', because some ADK serializers emit a bare object instead of
        //    an array when only one row is configured.
        final int firstBracket = table.indexOf('[');
        final int firstBrace = table.indexOf('{');
        if (firstBracket >= 0 || firstBrace >= 0) {
            final StringBuilder result = new StringBuilder(256);
            int objIdx = 0;
            while ((objIdx = table.indexOf('{', objIdx)) != -1) {
                final int endObjIdx = table.indexOf('}', objIdx);
                if (endObjIdx == -1) break;

                final String objSegment = table.substring(objIdx + 1, endObjIdx);
                final String host = extractJsonValue(objSegment, "kafkaHostRow");
                if (host != null && !host.isEmpty()) {
                    final String port = extractJsonValue(objSegment, "kafkaPortRow");
                    if (result.length() > 0) result.append(',');
                    result.append(host).append(':').append(port != null && !port.isEmpty() ? port : "9092");
                }
                objIdx = endObjIdx + 1;
            }
            if (result.length() > 0) {
                final String r = result.toString();
                resolvedBootstrapServersCache = r;
                return r;
            }
        }

        // 3. Last-resort plain-text fallback — handles cases where the ADK passes a raw
        //    "host:port" or "host:port,host2:port2" string directly with no XML/JSON wrapper
        //    (some ADK runtime versions do this when the table has exactly one row and the
        //    UI widget serializes it as a flat string instead of a structured table).
        if (table.indexOf(':') > 0 && table.indexOf('<') < 0 && table.indexOf('{') < 0) {
            resolvedBootstrapServersCache = table;
            return table;
        }

        // Nothing matched any known ADK serialization format. Surface the raw value length
        // and a safe preview (no full content, to avoid leaking credentials placed in the
        // same table by mistake) so this is diagnosable from CPI trace logs instead of
        // failing silently with "could not be resolved".
        resolvedBootstrapServersCache = "";
        org.slf4j.LoggerFactory.getLogger(SdiaKafkaEndpoint.class).warn(
                "[EventSmartKafka] kafkaClusterHostsTable did not match any known format "
                        + "(XML <row>, JSON array/object, or plain host:port). length=" + table.length()
                        + " preview=\"" + table.substring(0, Math.min(80, table.length())) + "\"");
        return null;
    }

    private static String extractXmlCellValue(final String rowSegment, final String cellId) {
        // Dual linear scan to support single and double quotes without compiling regex patterns.
        // Search for the attribute value anywhere on the cell tag — tolerant to extra attributes
        // appearing before or after id="..." (e.g. <cell id="kafkaHostRow" type="string">).
        int idIdx = rowSegment.indexOf("id='" + cellId + "'");
        if (idIdx == -1) {
            idIdx = rowSegment.indexOf("id=\"" + cellId + "\"");
        }
        if (idIdx == -1) return null;

        // Find the end of the *opening* cell tag (not just any '>'), so extra attributes
        // after id="..." don't get mistaken for the tag close when they contain '>' in a value.
        int closeTagIdx = rowSegment.indexOf('>', idIdx);
        if (closeTagIdx == -1) return null;

        int endTagIdx = rowSegment.indexOf("</cell>", closeTagIdx);
        if (endTagIdx == -1) return null;

        return rowSegment.substring(closeTagIdx + 1, endTagIdx).trim();
    }

    private static String extractXmlCellValueByIndex(final String rowSegment, final int targetIndex) {
        if (rowSegment == null || targetIndex < 0) return null;
        int searchIdx = 0;
        int cellIndex = 0;
        while ((searchIdx = rowSegment.indexOf("<cell", searchIdx)) != -1) {
            final int closeTagIdx = rowSegment.indexOf('>', searchIdx);
            if (closeTagIdx == -1) return null;
            final int endTagIdx = rowSegment.indexOf("</cell>", closeTagIdx);
            if (endTagIdx == -1) return null;
            if (cellIndex == targetIndex) {
                return rowSegment.substring(closeTagIdx + 1, endTagIdx).trim();
            }
            cellIndex++;
            searchIdx = endTagIdx + 7;
        }
        return null;
    }

    private static String extractJsonValue(final String objSegment, final String key) {
        int keyIdx = objSegment.indexOf('"' + key + '"');
        if (keyIdx == -1) return null;

        int colonIdx = objSegment.indexOf(':', keyIdx);
        if (colonIdx == -1) return null;

        int valStart = colonIdx + 1;
        final int len = objSegment.length();
        while (valStart < len && objSegment.charAt(valStart) <= ' ') {
            valStart++;
        }
        if (valStart >= len) return null;

        if (objSegment.charAt(valStart) == '"') {
            int valEnd = objSegment.indexOf('"', valStart + 1);
            return valEnd > valStart ? objSegment.substring(valStart + 1, valEnd).trim() : null;
        } else {
            int valEnd = valStart;
            while (valEnd < len && objSegment.charAt(valEnd) != ',' && objSegment.charAt(valEnd) != '}') {
                valEnd++;
            }
            return objSegment.substring(valStart, valEnd).trim();
        }
    }

    public boolean isSchemaIdFromMagicByte() {
        final String mode = trimToNull(this.getSchemaIdResolutionMode());
        final String fixedId = trimToNull(this.schemaRegistrySchemaId);

        if (mode == null) {
            // Defensive fallback: if the UI/binding did not send the mode but a fixed id exists,
            // treat the channel as Fixed Schema ID. Otherwise default to Magic Byte.
            return fixedId == null;
        }

        final String m = mode.toLowerCase(java.util.Locale.ROOT);
        if (m.indexOf("fixed") >= 0 || m.indexOf("schemaid") >= 0 || m.indexOf("schema id") >= 0) {
            return false;
        }
        if (m.indexOf("magic") >= 0 || m.indexOf("wire") >= 0 || m.indexOf("header") >= 0) {
            return true;
        }

        // Unknown mode: do not silently accept a populated Fixed Schema ID as Magic Byte.
        return fixedId == null;
    }

    /**
     * Runtime bridge for old and new metadata models.
     *
     * Old stable runtime uses conversionFormat=None/JSON/XML.
     * New metadata uses messageConversion=RAW_BYTES/AVRO_JSON/AVRO_XML.
     * This method prevents the adapter from silently passing Avro bytes through
     * when the design-time UI sends the newer field name.
     */
    public String getEffectiveConversionFormat() {
        final String legacyFormat = trimToNull(conversionFormat);
        if (legacyFormat != null && !"None".equalsIgnoreCase(legacyFormat)) {
            return legacyFormat;
        }

        final String semanticFormat = trimToNull(messageConversion);
        if (semanticFormat == null || "RAW_BYTES".equalsIgnoreCase(semanticFormat)) {
            return "None";
        }
        if ("AVRO_JSON".equalsIgnoreCase(semanticFormat)) {
            return "JSON";
        }
        if ("AVRO_XML".equalsIgnoreCase(semanticFormat)) {
            return "XML";
        }

        final String encoding = trimToNull(payloadEncoding);
        if ("JSON_PASSTHROUGH".equalsIgnoreCase(encoding)) {
            return "JSON_SCHEMA";
        }
        if ("RAW_BYTES".equalsIgnoreCase(encoding)) {
            return "None";
        }

        return legacyFormat != null ? legacyFormat : "None";
    }

    // ------------------------------------------------------------------
    // CONFIGURATION VALIDATION (fail-fast on deploy / first poll)
    // ------------------------------------------------------------------

    /**
     * Validates the endpoint configuration before the consumer starts.
     * Throws IllegalArgumentException with a clear, single-cause message so the
     * failure surfaces immediately in the CPI Operations Monitor instead of
     * producing obscure runtime errors later on.
     *
     * Covers the currently supported runtime paths:
     *   - Transport / discovery: resolvable bootstrap servers + a topic pattern.
     *   - Security: SASL over TLS (SASL_SSL) with Basic credentials (username/password
     *     supplied through credentialAlias); OAuth bearer is also accepted.
     *   - Conversion: Avro via Schema Registry, using either "Fixed Schema ID"
     *     or "Magic Byte" schema-id resolution.
     */
    public void validateConfiguration() {
        // --- Transport / topic discovery -----------------------------------
        if (trimToNull(resolveBootstrapServers()) == null) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Unable to resolve bootstrap servers. "
                            + "Provide a valid Kafka cluster hosts table (kafkaClusterHostsTable) "
                            + "or a host:port via firstUriPart.");
        }
        if (trimToNull(topicPattern) == null) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] 'topicPattern' is required and must not be empty.");
        }

        // --- Security ------------------------------------------------------
        validateSecurity();

        // --- Avro conversion + Schema Registry -----------------------------
        validateConversion();
    }

    /**
     * Validates the security configuration. Default authentication is SASL.
     * For SASL/Basic the credentialAlias (username/password) is mandatory and the
     * mechanism must be one of PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512. For OAuth the
     * token endpoint and client id are mandatory.
     */
    private void validateSecurity() {
        final String auth = trimToNull(authentication);

        // NONE / Plain Text path: no credentials, no TLS material, no SASL validation.
        // This is required for On-Premise/Cloud Connector/ngrok PLAINTEXT tests.
        if (isPlainTextAuthentication(auth)) {
            return;
        }

        // OAuth bearer path
        if (auth != null && "OAuth".equalsIgnoreCase(auth)) {
            if (trimToNull(oauthTokenEndpoint) == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] OAuth authentication requires 'oauthTokenEndpoint'.");
            }
            if (trimToNull(oauthClientId) == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] OAuth authentication requires 'oauthClientId'.");
            }
            return;
        }

        // SASL path (default when authentication is null/blank or any SASL profile is selected).
        // TLS is controlled independently by connectWithTls. Therefore both SASL_SSL
        // and SASL_PLAINTEXT require credentialAlias and the same mechanism validation.
        final boolean sasl = isSaslAuthentication(auth);
        if (sasl) {
            if (trimToNull(credentialAlias) == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] SASL authentication requires 'credentialAlias' "
                                + "(Basic username/password credential).");
            }
            final String mechanism = trimToNull(saslMechanism);
            if (mechanism != null
                    && !"PLAIN".equalsIgnoreCase(mechanism)
                    && !"SCRAM-SHA-256".equalsIgnoreCase(mechanism)
                    && !"SCRAM-SHA-512".equalsIgnoreCase(mechanism)) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] Unsupported SASL mechanism '" + mechanism
                                + "'. Use PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512.");
            }
        }
        // For non-SASL/non-OAuth (plain SSL / mTLS) no extra credential is required here.
    }

    /**
     * Validates the Avro conversion path. When a conversion format other than "None"
     * is selected, the Schema Registry host is mandatory. In "Fixed Schema ID" mode a
     * numeric schema id is required; in "Magic Byte" mode the id is read from the
     * record payload, so no fixed id is needed.
     */
    private void validateConversion() {
        final String format = trimToNull(getEffectiveConversionFormat());
        final boolean conversionRequested = (format != null) && !"None".equalsIgnoreCase(format);
        if (!conversionRequested) {
            return;
        }

        if (!"JSON".equalsIgnoreCase(format) && !"XML".equalsIgnoreCase(format)
                && !"JSON_SCHEMA".equalsIgnoreCase(format)) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Unsupported conversionFormat '" + format
                            + "'. Supported values: None, JSON, XML.");
        }

        if ("JSON_SCHEMA".equalsIgnoreCase(format)) {
            return;
        }

        if (trimToNull(schemaRegistryHostAddress) == null) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Conversion '" + format + "' requires "
                            + "'schemaRegistryHostAddress' to be configured.");
        }

        final boolean magicByte = isSchemaIdFromMagicByte();

        if (!magicByte) {
            final String fixedId = trimToNull(schemaRegistrySchemaId);
            if (fixedId == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] 'Fixed Schema ID' mode requires "
                                + "'schemaRegistrySchemaId' to be set.");
            }
            try {
                int fixed = Integer.parseInt(fixedId);
                if (fixed <= 0) {
                    throw new NumberFormatException("non-positive");
                }
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] 'schemaRegistrySchemaId' must be a positive numeric value, got: " + fixedId);
            }
        }

        // Force parsing at startup so metadata mistakes fail before the first Kafka record.
        getMaxPayloadSizeBytes();
        getSchemaCacheMaxBytes();
    }

    private boolean isSaslAuthentication(final String auth) {
        final String normalized = normalizeAuthenticationToken(auth);
        return normalized == null || normalized.indexOf("sasl") >= 0;
    }

    private boolean isPlainTextAuthentication(final String auth) {
        final String normalized = normalizeAuthenticationToken(auth);
        if (normalized == null) {
            return false;
        }

        // Accept the persisted value and the visible UI label variants.
        // NONE | Plain Text means: no SASL, no TLS, no CPI User Credential lookup.
        return "none".equals(normalized)
                || "plaintext".equals(normalized)
                || "plain text".equals(normalized)
                || "none plaintext".equals(normalized)
                || "none plain text".equals(normalized)
                || (normalized.indexOf("none") >= 0 && normalized.indexOf("plain") >= 0);
    }

    private String normalizeAuthenticationToken(final String value) {
        if (value == null) {
            return null;
        }
        final String raw = value.trim();
        if (raw.isEmpty()) {
            return null;
        }

        final StringBuilder sb = new StringBuilder(raw.length());
        boolean lastWasSpace = true;
        for (int i = 0; i < raw.length(); i++) {
            final char c = raw.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
                sb.append(c >= 'A' && c <= 'Z' ? (char) (c + 32) : c);
                lastWasSpace = false;
            } else if (!lastWasSpace) {
                sb.append(' ');
                lastWasSpace = true;
            }
        }

        final String normalized = sb.toString().trim();
        if (normalized.length() == 0) {
            return null;
        }
        return "plain text".equals(normalized) ? "plain text" : normalized;
    }

    /** Returns the trimmed value, or null when the input is null/blank. */
    private static String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    // ------------------------------------------------------------------
    // GETTERS & SETTERS (Cleaned up & Fast-path Null Protection)
    // ------------------------------------------------------------------

    public String getFirstUriPart() { return firstUriPart; }
    public void setFirstUriPart(String v) { this.firstUriPart = v; this.resolvedBootstrapServersCache = null; }

    public String getProxyType() { return proxyType; }
    public void setProxyType(String v) { this.proxyType = (v == null || v.isEmpty()) ? "Internet" : v; }

    public String getSapSapccLocationId() { return sapSapccLocationId; }
    public void setSapSapccLocationId(String v) { this.sapSapccLocationId = v; }

    public String getAuthentication() { return authentication; }
    public void setAuthentication(String v) { this.authentication = v; }

    public Boolean getConnectWithTls() { return connectWithTls; }
    public void setConnectWithTls(Boolean v) { this.connectWithTls = v != null ? v : Boolean.TRUE; }

    public String getSaslMechanism() { return saslMechanism; }
    public void setSaslMechanism(String v) { this.saslMechanism = v; }

    public String getCredentialAlias() { return credentialAlias; }
    public void setCredentialAlias(String v) { this.credentialAlias = v; }

    public String getCertificateAlias() { return certificateAlias; }
    public void setCertificateAlias(String v) { this.certificateAlias = v; }

    public String getBrokerCaSource() { return brokerCaSource; }
    public void setBrokerCaSource(String v) { this.brokerCaSource = (v == null || v.isEmpty()) ? "None" : v; }

    public String getTopicPattern() { return topicPattern; }
    public void setTopicPattern(String v) { this.topicPattern = v; }

    public String getPartitionAssignmentMode() { return partitionAssignmentMode; }
    public void setPartitionAssignmentMode(String v) {
        this.partitionAssignmentModeConfigured = true;
        this.partitionAssignmentMode = (v == null || v.trim().length() == 0) ? "Automatic Assignment" : v.trim();
    }

    public String getPartitions() {
        return isExplicitPartitionAssignment() ? partitions : null;
    }
    public void setPartitions(String v) { this.partitions = (v == null || v.trim().length() == 0) ? null : v.trim(); }

    private boolean isExplicitPartitionAssignment() {
        final String configuredPartitions = trimToNull(this.partitions);
        if (!partitionAssignmentModeConfigured && configuredPartitions != null) {
            // Backward compatibility for old deployed channels that had only the partitions field.
            return true;
        }
        final String mode = trimToNull(this.partitionAssignmentMode);
        if (mode == null) {
            return false;
        }
        final String m = mode.toLowerCase(java.util.Locale.ROOT);
        return m.indexOf("explicit") >= 0;
    }

    public Integer getParallelConsumers() { return parallelConsumers; }
    public void setParallelConsumers(Integer v) { this.parallelConsumers = v != null ? v : 1; }

    public String getMinFetchSize() { return String.valueOf(normalizeMinFetchSizeMb(minFetchSize)); }
    public void setMinFetchSize(Integer v) { this.minFetchSize = Integer.valueOf(normalizeMinFetchSizeMb(v)); }
    public void setMinFetchSize(String v) { this.minFetchSize = Integer.valueOf(normalizeMinFetchSizeMb(parseInteger(v))); }

    public String getMaxFetchSize() { return String.valueOf(normalizeFetchSizeMb(maxFetchSize, 20)); }
    public void setMaxFetchSize(Integer v) { this.maxFetchSize = Integer.valueOf(normalizeFetchSizeMb(v, 20)); }
    public void setMaxFetchSize(String v) { this.maxFetchSize = Integer.valueOf(normalizeFetchSizeMb(parseInteger(v), 20)); }

    /** Runtime Kafka value derived from metadata MB fixed profile. */
    public Integer getMinFetchSizeBytes() {
        int minMb = normalizeMinFetchSizeMb(minFetchSize);
        int maxMb = normalizeFetchSizeMb(maxFetchSize, 20);
        if (minMb > maxMb) {
            minMb = maxMb;
        }
        return Integer.valueOf(megabytesToBytes(minMb));
    }

    /** Runtime Kafka value derived from metadata MB field. */
    public Integer getMaxFetchSizeBytes() { return Integer.valueOf(megabytesToBytes(normalizeFetchSizeMb(maxFetchSize, 20))); }

    /**
     * Max Partition Fetch Size is intentionally no longer exposed in metadata.
     * Runtime derives it from Max Fetch Size so CPI users configure one MB-based fetch limit only.
     */
    public Integer getMaxPartitionFetchSize() { return maxPartitionFetchSize; }
    public void setMaxPartitionFetchSize(Integer v) { this.maxPartitionFetchSize = Integer.valueOf(normalizeFetchSizeMb(v, 20)); }
    public void setMaxPartitionFetchSize(String v) { this.maxPartitionFetchSize = Integer.valueOf(normalizeFetchSizeMb(parseInteger(v), 20)); }
    public Integer getMaxPartitionFetchSizeBytes() { return getMaxFetchSizeBytes(); }

    public Integer getMaxFetchWaitTime() { return Integer.valueOf(normalizeFetchWaitMs(maxFetchWaitTime)); }
    public void setMaxFetchWaitTime(Integer v) { this.maxFetchWaitTime = Integer.valueOf(normalizeFetchWaitMs(v)); }
    public void setMaxFetchWaitTime(String v) { this.maxFetchWaitTime = Integer.valueOf(normalizeFetchWaitMs(parseInteger(v))); }

    public Integer getMaxRetryBackoffMs() { return maxRetryBackoffMs; }
    public void setMaxRetryBackoffMs(Integer v) { this.maxRetryBackoffMs = positiveOrDefault(v, 1000); }
    public void setMaxRetryBackoffMs(String v) { this.maxRetryBackoffMs = positiveOrDefault(parseInteger(v), 1000); }

    public Long getReconnectDelayS() { return reconnectDelayS; }
    public void setReconnectDelayS(Long v) { this.reconnectDelayS = positiveOrDefault(v, 1L); }
    public void setReconnectDelayS(String v) { this.reconnectDelayS = positiveOrDefault(parseLong(v), 1L); }

    public Long getHeartbeatIntervalS() { return heartbeatIntervalS; }
    public void setHeartbeatIntervalS(Long v) { this.heartbeatIntervalS = positiveOrDefault(v, 3L); }
    public void setHeartbeatIntervalS(String v) { this.heartbeatIntervalS = positiveOrDefault(parseLong(v), 3L); }

    public Long getSessionTimeoutS() { return sessionTimeoutS; }
    public void setSessionTimeoutS(Long v) { this.sessionTimeoutS = positiveOrDefault(v, 10L); }
    public void setSessionTimeoutS(String v) { this.sessionTimeoutS = positiveOrDefault(parseLong(v), 10L); }

    public Integer getMaxPollIntervalMs() { return maxPollIntervalMs; }
    public void setMaxPollIntervalMs(Integer v) {
        int value = positiveOrDefault(v, 60000);
        if (value < 10000) value = 10000;
        if (value > 60000) value = 60000;
        this.maxPollIntervalMs = value;
    }
    public void setMaxPollIntervalMs(String v) { setMaxPollIntervalMs(parseInteger(v)); }

    public String getErrorHandling() { return errorHandling; }
    public void setErrorHandling(String v) { this.errorHandling = (v == null || v.isEmpty()) ? "Stop on Error" : v; }

    public Integer getRetryAttempts() { return retryAttempts; }
    public void setRetryAttempts(Integer v) {
        this.retryAttempts = (v != null && v.intValue() >= 5 && v.intValue() <= 10) ? v : 5;
    }

    public void setRetryAttempts(String v) {
        if (v == null) { this.retryAttempts = 5; return; }
        try {
            setRetryAttempts(Integer.valueOf(v.trim()));
        } catch (NumberFormatException ignored) {
            this.retryAttempts = 5;
        }
    }

    public String getConversionFormat() { return conversionFormat; }
    public void setConversionFormat(String v) { this.conversionFormat = (v == null || v.isEmpty()) ? "None" : v; }

    public String getMessageConversion() { return messageConversion; }
    public void setMessageConversion(String v) { this.messageConversion = (v == null || v.isEmpty()) ? "RAW_BYTES" : v; }


    public String getPayloadEncoding() { return payloadEncoding; }
    public void setPayloadEncoding(String v) { this.payloadEncoding = (v == null || v.isEmpty()) ? "AVRO_SCHEMA_REGISTRY_WIRE" : v; }

    public String getAvroOutputFormat() { return avroOutputFormat; }
    public void setAvroOutputFormat(String v) { this.avroOutputFormat = (v == null || v.isEmpty()) ? "JSON" : v; }

    public String getSchemaRegistryType() { return schemaRegistryType; }
    public void setSchemaRegistryType(String v) { this.schemaRegistryType = (v == null || v.isEmpty()) ? "CONFLUENT_KARAPACE" : v; }

    public String getSchemaIdResolutionMode() { return schemaIdResolutionMode; }
    public void setSchemaIdResolutionMode(String v) {
        this.schemaIdResolutionMode = (v == null || v.isEmpty()) ? "Magic Byte" : v.trim();
    }

    public String getSchemaRegistryHostAddress() { return schemaRegistryHostAddress; }
    public void setSchemaRegistryHostAddress(String v) { this.schemaRegistryHostAddress = v; }

    public String getSchemaRegistryCredentialAlias() { return schemaRegistryCredentialAlias; }
    public void setSchemaRegistryCredentialAlias(String v) { this.schemaRegistryCredentialAlias = v; }

    public String getSchemaRegistrySchemaId() { return schemaRegistrySchemaId; }
    public void setSchemaRegistrySchemaId(String v) { this.schemaRegistrySchemaId = v; }

    public Long getSchemaRegistryConnectTimeoutS() { return schemaRegistryConnectTimeoutS; }
    public void setSchemaRegistryConnectTimeoutS(Long v) { this.schemaRegistryConnectTimeoutS = positiveOrDefault(v, 5L); }
    public void setSchemaRegistryConnectTimeoutS(String v) { this.schemaRegistryConnectTimeoutS = positiveOrDefault(parseLong(v), 5L); }
    public Integer getSchemaRegistryConnectTimeoutMs() { return Integer.valueOf(safeSecondsToMillis(schemaRegistryConnectTimeoutS, 5)); }

    public Long getSchemaRegistryReadTimeoutS() { return schemaRegistryReadTimeoutS; }
    public void setSchemaRegistryReadTimeoutS(Long v) { this.schemaRegistryReadTimeoutS = positiveOrDefault(v, 10L); }
    public void setSchemaRegistryReadTimeoutS(String v) { this.schemaRegistryReadTimeoutS = positiveOrDefault(parseLong(v), 10L); }
    public Integer getSchemaRegistryReadTimeoutMs() { return Integer.valueOf(safeSecondsToMillis(schemaRegistryReadTimeoutS, 10)); }

    public String getMaxPayloadConversionSize() { return maxPayloadConversionSize; }
    public void setMaxPayloadConversionSize(String v) {
        final Integer parsed = parseInteger(v);
        final int mb = normalizePayloadMb(parsed);
        this.maxPayloadSizeMb = Integer.valueOf(mb);
        this.maxPayloadConversionSize = String.valueOf(mb);
    }

    public String getSchemaIdBufferSize() { return schemaIdBufferSize; }
    public void setSchemaIdBufferSize(String v) {
        final Integer parsed = parseInteger(v);
        final int kb = normalizeSchemaCacheKb(parsed);
        this.schemaIdSizeBuffer = Integer.valueOf(kb);
        this.schemaIdBufferSize = String.valueOf(kb);
    }

    /*
     * Backward-compatible aliases used by older runtime code. The public metadata
     * names are maxPayloadConversionSize and schemaIdBufferSize.
     */
    public Integer getMaxPayloadSizeMb() { return maxPayloadSizeMb; }
    public void setMaxPayloadSizeMb(Integer v) {
        final int mb = normalizePayloadMb(v);
        this.maxPayloadSizeMb = Integer.valueOf(mb);
        this.maxPayloadConversionSize = String.valueOf(mb);
    }

    public void setMaxPayloadSizeMb(String v) {
        setMaxPayloadConversionSize(v);
    }

    public Integer getSchemaIdSizeBuffer() { return schemaIdSizeBuffer; }
    public void setSchemaIdSizeBuffer(Integer v) {
        final int kb = normalizeSchemaCacheKb(v);
        this.schemaIdSizeBuffer = Integer.valueOf(kb);
        this.schemaIdBufferSize = String.valueOf(kb);
    }

    public void setSchemaIdSizeBuffer(String v) {
        setSchemaIdBufferSize(v);
    }

    public long getMaxPayloadSizeBytes() {
        int mb = normalizePayloadMb(this.maxPayloadSizeMb);
        return ((long) mb) * 1024L * 1024L;
    }

    /**
     * Hex preview size for error diagnostics. This is intentionally small and is
     * not the same thing as schemaIdBufferSize, which controls max schema JSON KB.
     */
    public int getEffectiveSchemaIdSizeBuffer() {
        return 16;
    }

    public int getSchemaCacheMaxBytes() {
        int kb = normalizeSchemaCacheKb(this.schemaIdSizeBuffer);
        return kb * 1024;
    }

    public Long getPollTimeoutS() { return pollTimeoutS; }
    public void setPollTimeoutS(Long v) {
        this.pollTimeoutS = (v == null || v.longValue() < 1L) ? 1L : v;
        this.pollTimeoutMs = this.pollTimeoutS * 1000L;
    }

    public Long getPollTimeoutMs() { return pollTimeoutMs; }
    public void setPollTimeoutMs(Long v) {
        this.pollTimeoutMs = (v == null || v.longValue() < 1L) ? 1000L : v;
        this.pollTimeoutS = Math.max(1L, this.pollTimeoutMs / 1000L);
    }

    public Long getInitialDelayS() { return initialDelayS; }
    public void setInitialDelayS(Long v) {
        this.initialDelayS = 0L;
        super.setInitialDelay(0L);
    }

    public Long getDelayS() { return delayS; }
    public void setDelayS(Long v) {
        this.delayS = 1L;
        super.setDelay(1000L);
    }

    public Integer getMaxPollRecords() { return Integer.valueOf(normalizeMaxPollRecords(maxPollRecords)); }
    public void setMaxPollRecords(Integer v) { this.maxPollRecords = Integer.valueOf(normalizeMaxPollRecords(v)); }
    public void setMaxPollRecords(String v) { this.maxPollRecords = Integer.valueOf(normalizeMaxPollRecords(parseInteger(v))); }

    public String getGroupId() { return groupId; }
    public void setGroupId(String v) { this.groupId = v; }

    public String getAutoOffsetReset() { return autoOffsetReset; }
    public void setAutoOffsetReset(String v) { this.autoOffsetReset = v; }

    public Long getRequestTimeoutS() { return requestTimeoutS; }
    public void setRequestTimeoutS(Long v) {
        this.requestTimeoutS = (v == null || v.longValue() < 1L) ? 30L : v;
        this.requestTimeoutMs = this.requestTimeoutS * 1000L;
    }

    public Long getRequestTimeoutMs() { return requestTimeoutMs; }
    public void setRequestTimeoutMs(Long v) {
        this.requestTimeoutMs = (v == null || v.longValue() < 1L) ? 30000L : v;
        this.requestTimeoutS = Math.max(1L, this.requestTimeoutMs / 1000L);
    }


    public String getKafkaClusterHostsTable() { return kafkaClusterHostsTable; }
    public void setKafkaClusterHostsTable(String v) { this.kafkaClusterHostsTable = v; this.resolvedBootstrapServersCache = null; }

    public String getKafkaHostRow() { return kafkaHostRow; }
    public void setKafkaHostRow(String v) { this.kafkaHostRow = v; this.resolvedBootstrapServersCache = null; }

    public String getKafkaPortRow() { return kafkaPortRow; }
    public void setKafkaPortRow(String v) { this.kafkaPortRow = v; this.resolvedBootstrapServersCache = null; }

    public String getOauthTokenEndpoint() { return oauthTokenEndpoint; }
    public void setOauthTokenEndpoint(String v) { this.oauthTokenEndpoint = v; }

    public String getOauthClientId() { return oauthClientId; }
    public void setOauthClientId(String v) { this.oauthClientId = v; }

    public String getOauthClientSecretAlias() { return oauthClientSecretAlias; }
    public void setOauthClientSecretAlias(String v) { this.oauthClientSecretAlias = v; }

    public String getOauthScope() { return oauthScope; }
    public void setOauthScope(String v) { this.oauthScope = v; }

    public String getMtlsKeyAlias() { return mtlsKeyAlias; }
    public void setMtlsKeyAlias(String v) { this.mtlsKeyAlias = v; }

    public String getSeekToTimestamp() { return seekToTimestamp; }
    public void setSeekToTimestamp(String v) {
        this.seekToTimestamp = (v != null && v.trim().isEmpty()) ? null : v;
    }

    public String getRecoveryMode() { return recoveryMode; }
    public void setRecoveryMode(String v) {
        this.recoveryMode = (v != null && v.trim().length() > 0) ? v.trim() : "Disabled";
    }

    public String getRecoveryTimestampFormat() { return recoveryTimestampFormat; }
    public void setRecoveryTimestampFormat(String v) {
        this.recoveryTimestampFormat = (v != null && v.trim().length() > 0) ? v.trim() : "Auto Detect";
    }

    public String getRecoveryTimestampValue() { return recoveryTimestampValue; }
    public void setRecoveryTimestampValue(String v) {
        this.recoveryTimestampValue = (v != null && v.trim().length() > 0) ? v.trim() : null;
    }

    public String getRecoveryReadMode() { return recoveryReadMode; }
    public void setRecoveryReadMode(String v) {
        this.recoveryReadMode = (v != null && v.trim().length() > 0) ? v.trim() : "Read only first matched message";
    }

    public boolean isRecoverySingleMessageOnly() {
        return recoveryReadMode == null
                || recoveryReadMode.trim().length() == 0
                || "Read only first matched message".equalsIgnoreCase(recoveryReadMode.trim())
                || "Single Message Only".equalsIgnoreCase(recoveryReadMode.trim())
                || "Read only first matched record".equalsIgnoreCase(recoveryReadMode.trim());
    }

    public boolean isMessageRecoveryEnabled() {
        return recoveryMode != null && "Seek by Timestamp".equalsIgnoreCase(recoveryMode.trim());
    }

    public long getSeekToTimestampMs() {
        if (isMessageRecoveryEnabled()) {
            return parseRecoveryTimestampToMillis(recoveryTimestampValue, recoveryTimestampFormat);
        }

        // Legacy field kept for backward compatibility with older channel configurations.
        return parseRecoveryTimestampToMillis(seekToTimestamp, "Unix Epoch milliseconds");
    }

    private long parseRecoveryTimestampToMillis(final String rawValue, final String rawFormat) {
        if (rawValue == null || rawValue.trim().length() == 0) {
            return -1L;
        }

        final String value = rawValue.trim();
        final String format = rawFormat != null ? rawFormat.trim() : "Auto Detect";

        try {
            if ("ISO-8601 UTC".equalsIgnoreCase(format)) {
                return java.time.Instant.parse(value).toEpochMilli();
            }
            if ("Unix Epoch seconds".equalsIgnoreCase(format)) {
                return Long.parseLong(value) * 1000L;
            }
            if ("Unix Epoch milliseconds".equalsIgnoreCase(format)) {
                return Long.parseLong(value);
            }

            // Auto Detect. Accept direct copy from Confluent UI, epoch ms, or epoch seconds.
            if (value.indexOf('T') >= 0 || value.endsWith("Z")) {
                return java.time.Instant.parse(value).toEpochMilli();
            }

            final String digitsOnly = value.startsWith("-") ? value.substring(1) : value;
            if (!digitsOnly.matches("\\d+")) {
                return -1L;
            }

            final long parsed = Long.parseLong(value);
            if (digitsOnly.length() <= 10) {
                return parsed * 1000L;
            }
            return parsed;
        } catch (Throwable ignored) {
            return -1L;
        }
    }
    private static Integer parseInteger(final String value) {
        if (value == null) return null;
        try {
            final String trimmed = value.trim();
            if (trimmed.length() == 0) return null;
            return Integer.valueOf(trimmed);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLong(final String value) {
        if (value == null) return null;
        try {
            final String trimmed = value.trim();
            if (trimmed.length() == 0) return null;
            return Long.valueOf(trimmed);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static int positiveOrDefault(final Integer value, final int def) {
        return value != null && value.intValue() > 0 ? value.intValue() : def;
    }

    private static long positiveOrDefault(final Long value, final long def) {
        return value != null && value.longValue() > 0L ? value.longValue() : def;
    }

    private static int normalizePayloadMb(final Integer value) {
        final int mb = value == null ? 1 : value.intValue();
        if (mb <= 1) return 1;
        return 2;
    }

    /**
     * Min Fetch Size is a fixed metadata profile: 1, 5, 10 or 20 MB.
     * Older deployed iFlows may still send bytes from previous metadata versions;
     * values greater than 1024 are interpreted as bytes and converted back to MB.
     */
    private static int normalizeMinFetchSizeMb(final Integer value) {
        return normalizeFixedFetchProfileMb(value, 1);
    }

    /**
     * Metadata now exposes fetch sizes in MB. Older deployed iFlows may still send bytes
     * from previous metadata versions; values greater than 1024 are interpreted as bytes
     * and converted back to MB. Runtime clamp: 1 MB to 20 MB.
     */
    private static int normalizeFetchSizeMb(final Integer value, final int defaultMb) {
        return normalizeFixedFetchProfileMb(value, defaultMb);
    }

    private static int normalizeFixedFetchProfileMb(final Integer value, final int defaultMb) {
        long raw = value == null ? defaultMb : value.longValue();
        if (raw <= 0L) {
            raw = defaultMb;
        }
        if (raw > 1024L) {
            raw = (raw + 1048575L) / 1048576L;
        }
        if (raw == 1L || raw == 5L || raw == 10L || raw == 20L) {
            return (int) raw;
        }
        if (raw > 20L) {
            return 20;
        }
        throw new IllegalArgumentException("Invalid Fetch Size. Allowed values: 1, 5, 10 or 20 MB.");
    }

    private static int normalizeMaxPollRecords(final Integer value) {
        final int records = value == null ? 10 : value.intValue();
        if (records < 1) return 1;
        if (records > 500) return 500;
        return records;
    }

    private static int normalizeFetchWaitMs(final Integer value) {
        int ms = value == null ? 500 : value.intValue();
        // Historical channel versions exposed this field and may have persisted 300000 ms,
        // causing the observed 5-minute zombie wait. Keep it hidden and hard-clamp it.
        if (ms < 50) return 50;
        if (ms > 1000) return 500;
        return ms;
    }

    private static int safeSecondsToMillis(final Long seconds, final int defaultSeconds) {
        long s = seconds == null || seconds.longValue() < 1L ? defaultSeconds : seconds.longValue();
        if (s > 300L) s = 300L;
        return (int) (s * 1000L);
    }

    private static int megabytesToBytes(final int mb) {
        return mb * 1024 * 1024;
    }

    private static int normalizeSchemaCacheKb(final Integer value) {
        final int kb = value == null ? 5 : value.intValue();
        if (kb <= 5) return 5;
        if (kb <= 10) return 10;
        return 15;
    }

}