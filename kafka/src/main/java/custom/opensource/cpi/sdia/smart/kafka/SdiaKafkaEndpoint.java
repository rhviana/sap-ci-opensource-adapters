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

import java.util.Locale;
import java.util.regex.Pattern;

/**
 * SAP CPI ADK endpoint for the Event Smart Kafka adapter.
 *
 * <h3>TLS trust sources (v1.0.1)</h3>
 * <ul>
 *   <li><b>JVM_DEFAULT</b> — uses the JVM built-in truststore. Correct for
 *       Confluent Cloud, Aiven, MSK, and any broker signed by a well-known CA.</li>
 *   <li><b>PEM_CONTENT</b> — operator pastes the broker CA or server certificate
 *       in PEM format directly into the channel. Correct for self-signed / private
 *       CA brokers (e.g. local Docker Kafka with {@code kafka.server.crt}).</li>
 *   <li><b>CPI_TRUSTED_CERTIFICATE_ALIAS</b> — loads the trusted certificate from
 *       the SAP CPI Keystore by alias. Prepared for future implementation; throws
 *       a fast-fail error when selected in v1.0.</li>
 * </ul>
 *
 * <h3>Security protocol matrix (v1.0.1)</h3>
 * <pre>
 * Authentication=NONE  + TLS=false → PLAINTEXT         (no credentials, no SSL)
 * Authentication=NONE  + TLS=true  → SSL               (no credentials, SSL trust only)
 * Authentication=SASL  + TLS=false → SASL_PLAINTEXT    (credentials, no SSL)
 * Authentication=SASL  + TLS=true  → SASL_SSL          (credentials + SSL trust)
 * </pre>
 *
 * <h3>Payload limits</h3>
 * <ul>
 *   <li>Avro conversion: configurable 1–5 MB via {@code maxPayloadConversionSize}.</li>
 *   <li>Raw pass-through: hard 20 MB limit enforced in the consumer.</li>
 *   <li>Schema JSON cache size: configurable via {@code schemaIdBufferSize} (1, 50, 75, or 100 KB).</li>
 *   <li>Schema JSON cache TTL: configurable via {@code schemaCacheTtlH} (1 or 2 hours).</li>
 * </ul>
 */
@UriEndpoint(
        scheme = "eventsmartkafka",
        syntax = "eventsmartkafka://kafka",
        title  = "EventSmartKafka"
)
public class SdiaKafkaEndpoint extends DefaultPollingEndpoint {

    /** Hard limit for raw (non-converted) payload pass-through. */
    static final long RAW_PAYLOAD_HARD_LIMIT_BYTES = 20L * 1024L * 1024L; // 20 MB

    /** Compiled once — used in parseRecoveryTimestampToMillis for digits-only check. */
    private static final Pattern DIGITS_PATTERN = Pattern.compile("\\d+");

    private SdiaKafkaComponent component;

    // ── Bootstrap / connectivity ─────────────────────────────────────────────
    @UriParam private String  firstUriPart;
    @UriParam private String  kafkaClusterHostsTable;
    @UriParam private String  kafkaHostRow;
    @UriParam private String  kafkaPortRow;          // String for CPI external-parameter binding
    @UriParam private String  proxyType = "Internet";
    @UriParam private String  sapSapccLocationId;

    // ── Authentication ───────────────────────────────────────────────────────
    @UriParam private String  authentication = "SASL";
    @UriParam private String  saslMechanism  = "PLAIN";
    @UriParam private String  credentialAlias;

    // ── TLS — existing field (kept for backward compat) ──────────────────────
    /**
     * Existing boolean TLS toggle. Maps to the "Connect with TLS" channel checkbox.
     * Use {@link #isTlsEnabledEffective()} in all logic paths.
     */
    @UriParam private Boolean connectWithTls = Boolean.TRUE;

    // ── TLS — new trust source fields (v1.0.1) ────────────────────────────────

    /**
     * Controls which trust material the Kafka client uses for TLS.
     * Values: {@code JVM_DEFAULT}, {@code PEM_CONTENT}, {@code CPI_TRUSTED_CERTIFICATE_ALIAS}.
     * Default: {@code JVM_DEFAULT}.
     * Only active when {@link #connectWithTls} is {@code true}.
     */
    @UriParam private String  tlsTrustSource = "JVM_DEFAULT";

    /**
     * Trusted CA certificate (or broker server certificate) in PEM format.
     * Active only when {@link #tlsTrustSource} is {@code PEM_CONTENT}.
     * Must contain {@code -----BEGIN CERTIFICATE-----} and
     * {@code -----END CERTIFICATE-----}. Must NOT contain any private key.
     */
    @UriParam private String  tlsTrustedCaPem;

    /**
     * SAP CPI Keystore alias holding the trusted CA certificate.
     * Active only when {@link #tlsTrustSource} is {@code CPI_TRUSTED_CERTIFICATE_ALIAS}.
     * Reserved for a future release — throws a fast-fail error when selected in v1.0.
     */
    @UriParam private String  tlsTrustedCertificateAlias;

    // ── Legacy trust fields (kept for backward compat, deprecated) ───────────
    /**
     * @deprecated Use {@link #tlsTrustSource} + {@link #tlsTrustedCertificateAlias}.
     *             Retained so existing deployed channels continue to bind without error.
     */
    @Deprecated
    @UriParam private String  certificateAlias;

    /**
     * @deprecated Replaced by {@link #tlsTrustSource} dropdown.
     *             Retained so existing deployed channels continue to bind without error.
     */
    @Deprecated
    @UriParam private String  brokerCaSource = "None";

    // ── mTLS — reserved, not active in v1.0 ─────────────────────────────────
    /** Reserved for mTLS in a future release. Not used in v1.0 security logic. */
    @UriParam private String  mtlsKeyAlias;

    // ── OAuth — reserved, not active in v1.0 ─────────────────────────────────
    @UriParam private String  oauthTokenEndpoint;
    @UriParam private String  oauthClientId;
    @UriParam private String  oauthClientSecretAlias;
    @UriParam private String  oauthScope;

    // ── Topic / consumer ─────────────────────────────────────────────────────
    @UriParam private String  topicPattern;
    @UriParam private String  partitions;
    @UriParam private Integer parallelConsumers = 1;
    @UriParam private Integer minFetchSize      = 0;
    @UriParam private Integer maxFetchSize      = 5;
    @UriParam private Integer maxPartitionFetchSize = null;
    @UriParam private Integer maxFetchWaitTime  = 500;
    @UriParam private Integer maxRetryBackoffMs = 1000;
    @UriParam private Long    reconnectDelayS   = 1L;
    @UriParam private Long    heartbeatIntervalS = 3L;
    @UriParam private Long    sessionTimeoutS   = 10L;
    @UriParam private Integer maxPollIntervalMs = 60000;
    @UriParam private String  errorHandling     = "Stop on Error";
    @UriParam private Integer retryAttempts     = 5;

    // ── Conversion / Schema Registry ─────────────────────────────────────────
    @UriParam private String  conversionFormat  = "None";
    @UriParam private String  messageConversion = "RAW_BYTES";
    @UriParam private String  producerRegistryProfile  = "CONFLUENT_CLOUD";
    @UriParam private String  payloadEncoding   = "AVRO_SCHEMA_REGISTRY_WIRE";
    @UriParam private String  avroOutputFormat  = "JSON";
    @UriParam private String  schemaRegistryType = "CONFLUENT_KARAPACE";
    @UriParam private String  schemaIdResolutionMode = "Magic Byte";
    @UriParam private String  schemaRegistryHostAddress;
    @UriParam private String  schemaRegistryCredentialAlias;
    @UriParam private String  schemaRegistrySchemaId;
    @UriParam private String  maxPayloadConversionSize = "1";
    @UriParam private String  schemaIdBufferSize = "50";
    @UriParam private Integer maxPayloadSizeMb   = 1;
    @UriParam private Integer schemaIdSizeBuffer = 50;

    /**
     * Schema Registry cache TTL in hours.
     * Valid values: 1 or 2.
     * Default: 1 hour.
     *
     * Schema IDs in Confluent Schema Registry are immutable — once registered,
     * an ID always maps to the same JSON. A 2-hour TTL is safe and reduces
     * Schema Registry network calls by half during long-running deployments.
     * Magic Byte mode and Fixed Schema ID mode both use this TTL.
     */
    @UriParam private Integer schemaCacheTtlH = 1;

    // ── Polling / scheduling ─────────────────────────────────────────────────
    @UriParam private Long    pollTimeoutS  = 1L;
    @UriParam private Long    pollTimeoutMs = 1000L;
    @UriParam private Long    initialDelayS = 0L;
    @UriParam private Long    delayS        = 1L;
    @UriParam private Integer maxPollRecords = 10;
    @UriParam private String  groupId;
    @UriParam private String  autoOffsetReset = "earliest";

    // ── Message Recovery ─────────────────────────────────────────────────────
    @UriParam private String  seekToTimestamp;
    @UriParam private String  recoveryMode          = "Disabled";
    @UriParam private String  recoveryTimestampFormat = "Auto Detect";
    @UriParam private String  recoveryTimestampValue;
    @UriParam private String  recoveryReadMode      = "Read only first matched message";

    // ── Producer ─────────────────────────────────────────────────────────────
    @UriParam private String  targetTopic;
    @UriParam private String  keyHeaderName;
    @UriParam private String  acks          = "all";
    @UriParam private Long    requestTimeoutS  = 30L;
    @UriParam private Long    requestTimeoutMs = 30000L;
    @UriParam private Long    deliveryTimeoutS  = 120L;
    @UriParam private Long    deliveryTimeoutMs = 120000L;

    // =========================================================================
    // Constructors
    // =========================================================================

    public SdiaKafkaEndpoint() { applySchedulingDefaults(); }

    public SdiaKafkaEndpoint(final String endpointUri, final SdiaKafkaComponent component) {
        super(endpointUri, component);
        this.component = component;
        applySchedulingDefaults();
    }

    private void applySchedulingDefaults() {
        this.initialDelayS = 0L;
        this.delayS        = 1L;
        super.setInitialDelay(0L);
        super.setDelay(1000L);
    }

    @Override public Producer createProducer() throws Exception { return new SdiaKafkaProducer(this); }

    @Override
    public Consumer createConsumer(final Processor processor) throws Exception {
        final SdiaKafkaConsumer consumer = new SdiaKafkaConsumer(this, processor);
        configureConsumer(consumer);
        return consumer;
    }

    @Override public boolean isSingleton() { return true; }

    // =========================================================================
    // Bootstrap resolution
    // =========================================================================

    public String resolveBootstrapServers() {
        final String table = kafkaClusterHostsTable == null ? null : kafkaClusterHostsTable.trim();
        if (table == null || table.isEmpty()) {
            final String legacy = firstUriPart == null ? null : firstUriPart.trim();
            return (legacy != null && !legacy.isEmpty()
                    && !"foo".equalsIgnoreCase(legacy)
                    && !"kafka".equalsIgnoreCase(legacy)) ? legacy : null;
        }

        if (table.contains("<row>") || table.contains("<cell")) {
            final StringBuilder result = new StringBuilder(256);
            int rowIdx = 0;
            while ((rowIdx = table.indexOf("<row>", rowIdx)) != -1) {
                final int endRowIdx = table.indexOf("</row>", rowIdx);
                if (endRowIdx == -1) break;
                final String seg  = table.substring(rowIdx + 5, endRowIdx);
                final String host = extractXmlCellValue(seg, "kafkaHostRow");
                if (host != null && !host.isEmpty()) {
                    final String port = extractXmlCellValue(seg, "kafkaPortRow");
                    if (result.length() > 0) result.append(',');
                    result.append(host).append(':').append(port != null && !port.isEmpty() ? port : "9092");
                }
                rowIdx = endRowIdx + 6;
            }
            if (result.length() > 0) return result.toString();
        }

        if (!table.isEmpty() && table.charAt(0) == '[') {
            final StringBuilder result = new StringBuilder(256);
            int objIdx = 0;
            while ((objIdx = table.indexOf('{', objIdx)) != -1) {
                final int endObjIdx = table.indexOf('}', objIdx);
                if (endObjIdx == -1) break;
                final String seg  = table.substring(objIdx + 1, endObjIdx);
                final String host = extractJsonValue(seg, "kafkaHostRow");
                if (host != null && !host.isEmpty()) {
                    final String port = extractJsonValue(seg, "kafkaPortRow");
                    if (result.length() > 0) result.append(',');
                    result.append(host).append(':').append(port != null && !port.isEmpty() ? port : "9092");
                }
                objIdx = endObjIdx + 1;
            }
            if (result.length() > 0) return result.toString();
        }
        return null;
    }

    private static String extractXmlCellValue(final String seg, final String cellId) {
        int idx = seg.indexOf("id='" + cellId + "'");
        if (idx == -1) idx = seg.indexOf("id=\"" + cellId + "\"");
        if (idx == -1) return null;
        final int close = seg.indexOf('>', idx);
        if (close == -1) return null;
        final int end = seg.indexOf("</cell>", close);
        if (end == -1) return null;
        return seg.substring(close + 1, end).trim();
    }

    private static String extractJsonValue(final String seg, final String key) {
        final int keyIdx = seg.indexOf('"' + key + '"');
        if (keyIdx == -1) return null;
        final int colonIdx = seg.indexOf(':', keyIdx);
        if (colonIdx == -1) return null;
        int start = colonIdx + 1;
        while (start < seg.length() && seg.charAt(start) <= ' ') start++;
        if (start >= seg.length()) return null;
        if (seg.charAt(start) == '"') {
            final int end = seg.indexOf('"', start + 1);
            return end > start ? seg.substring(start + 1, end).trim() : null;
        }
        int end = start;
        while (end < seg.length() && seg.charAt(end) != ',' && seg.charAt(end) != '}') end++;
        return seg.substring(start, end).trim();
    }

    // =========================================================================
    // TLS helpers — primary API for security configuration
    // =========================================================================

    /**
     * Returns {@code true} when TLS is enabled for this channel.
     * Authoritative source for all security-protocol derivation logic.
     */
    public boolean isTlsEnabledEffective() {
        return Boolean.TRUE.equals(this.connectWithTls);
    }

    /**
     * Returns the normalised TLS trust source: {@code JVM_DEFAULT},
     * {@code PEM_CONTENT}, or {@code CPI_TRUSTED_CERTIFICATE_ALIAS}.
     *
     * <p>Falls back to {@code JVM_DEFAULT} on null/empty/unrecognised input
     * so that existing deployed channels without this field behave correctly.
     *
     * <p>Backward compatibility: if the legacy {@code brokerCaSource} is
     * {@code "Custom"} and the new {@code tlsTrustSource} is not set, returns
     * {@code CPI_TRUSTED_CERTIFICATE_ALIAS} so that existing channels using the
     * old keystore-alias model continue to work during migration.
     */
    public String getTlsTrustSourceEffective() {
        final String newValue = trimToNull(this.tlsTrustSource);
        if (newValue != null) {
            final String upper = newValue.toUpperCase(Locale.ROOT);
            if ("JVM_DEFAULT".equals(upper)
                    || "PEM_CONTENT".equals(upper)
                    || "CPI_TRUSTED_CERTIFICATE_ALIAS".equals(upper)) {
                return upper;
            }
        }

        // Backward compat: if legacy brokerCaSource=Custom is set, map to alias mode.
        final String legacy = trimToNull(this.brokerCaSource);
        if ("Custom".equalsIgnoreCase(legacy)) {
            return "CPI_TRUSTED_CERTIFICATE_ALIAS";
        }

        return "JVM_DEFAULT";
    }

    /**
     * Returns {@code true} when the authentication profile requires SASL.
     * Used exclusively for credential-resolution gating.
     */
    public boolean isAuthenticationSaslEffective() {
        final String n = normalizeAuthenticationToken(this.authentication);
        return n != null && n.contains("sasl");
    }

    /**
     * Returns the effective trusted CA PEM string.
     * Returns the new {@code tlsTrustedCaPem} field if set; otherwise returns
     * {@code null} (no PEM configured).
     */
    public String getEffectiveTrustedCaPem() {
        return trimToNull(this.tlsTrustedCaPem);
    }

    /**
     * Returns the effective CPI Keystore alias for the trusted CA certificate.
     * Prefers the new {@code tlsTrustedCertificateAlias} field; falls back to
     * the legacy {@code certificateAlias} field for backward compatibility.
     */
    public String getEffectiveTrustedCertificateAlias() {
        final String newAlias = trimToNull(this.tlsTrustedCertificateAlias);
        if (newAlias != null) {
            return newAlias;
        }
        // Legacy fallback: certificateAlias was used when brokerCaSource=Custom
        return trimToNull(this.certificateAlias);
    }

    // =========================================================================
    // Schema ID / conversion helpers
    // =========================================================================

    public boolean isSchemaIdFromMagicByte() {
        final String mode    = trimToNull(schemaIdResolutionMode);
        final String fixedId = trimToNull(schemaRegistrySchemaId);
        if (mode == null) return fixedId == null;
        final String m = mode.toLowerCase(Locale.ROOT);
        if (m.contains("fixed") || m.contains("schemaid") || m.contains("schema id")) return false;
        if (m.contains("magic") || m.contains("wire")     || m.contains("header"))    return true;
        return fixedId == null;
    }

    public String getEffectiveConversionFormat() {
        final String legacy = trimToNull(conversionFormat);
        if (legacy != null && !"None".equalsIgnoreCase(legacy)) return legacy;
        final String semantic = trimToNull(messageConversion);
        if (semantic == null || "RAW_BYTES".equalsIgnoreCase(semantic)) return "None";
        if ("AVRO_JSON".equalsIgnoreCase(semantic)) return "JSON";
        if ("AVRO_XML".equalsIgnoreCase(semantic))  return "XML";
        final String encoding = trimToNull(payloadEncoding);
        if ("JSON_PASSTHROUGH".equalsIgnoreCase(encoding)) return "JSON_SCHEMA";
        if ("RAW_BYTES".equalsIgnoreCase(encoding))        return "None";
        return legacy != null ? legacy : "None";
    }

    // =========================================================================
    // Validation
    // =========================================================================

    public void validateConfiguration() {
        if (trimToNull(resolveBootstrapServers()) == null) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Unable to resolve bootstrap servers. "
                    + "Provide a valid Kafka cluster hosts table (kafkaClusterHostsTable) "
                    + "or a host:port via firstUriPart.");
        }
        if (trimToNull(topicPattern) == null) {
            throw new IllegalArgumentException("[SDIA Kafka] 'topicPattern' is required and must not be empty.");
        }
        validateSecurity();
        validateTlsTrustSource();
        validateConversion();
    }

    /**
     * Validates authentication-related configuration.
     *
     * <h3>Critical NONE contract</h3>
     * When authentication is {@code NONE | PLAINTEXT}, no credential alias
     * validation is performed — regardless of proxy type or TLS state.
     * Credential validation is strictly SASL-only.
     */
    private void validateSecurity() {
        final String auth = trimToNull(authentication);

        // NONE / PLAINTEXT: credential is never required.
        if (isPlainTextAuthenticationInternal(auth)) {
            return;
        }

        // SASL: credential alias is mandatory.
        if (isAuthenticationSaslEffective()) {
            if (trimToNull(credentialAlias) == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] Authentication SASL requires a Credential Alias. "
                        + "Deploy a User Credentials Security Material and set 'credentialAlias'.");
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
    }

    /**
     * Validates TLS trust source configuration.
     * Called only when TLS is relevant; safe to call unconditionally (no-ops
     * when TLS is disabled or trust source is JVM_DEFAULT).
     */
    private void validateTlsTrustSource() {
        if (!isTlsEnabledEffective()) {
            return; // TLS disabled — trust source irrelevant
        }

        final String trustSource = getTlsTrustSourceEffective();

        if ("PEM_CONTENT".equals(trustSource)) {
            final String pem = trimToNull(tlsTrustedCaPem);
            if (pem == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] TLS is enabled and TLS Trust Source is PEM_CONTENT, "
                        + "but Trusted CA PEM is empty. Paste the broker CA or server certificate in PEM format.");
            }
            if (pem.contains("PRIVATE KEY")) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] Trusted CA PEM must contain only certificates. "
                        + "Private keys are not allowed.");
            }
            if (!pem.contains("-----BEGIN CERTIFICATE-----")
                    || !pem.contains("-----END CERTIFICATE-----")) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] Trusted CA PEM must contain a valid PEM certificate block. "
                        + "Expected: -----BEGIN CERTIFICATE----- ... -----END CERTIFICATE-----");
            }
        }

        if ("CPI_TRUSTED_CERTIFICATE_ALIAS".equals(trustSource)) {
            final String alias = getEffectiveTrustedCertificateAlias();
            if (alias == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] TLS Trust Source is CPI_TRUSTED_CERTIFICATE_ALIAS "
                        + "but no certificate alias is configured. "
                        + "Set 'tlsTrustedCertificateAlias' to a valid CPI Keystore alias.");
            }
            // Alias is present — the configurator will attempt to load it.
            // Fast-fail for unimplemented path happens inside SdiaKafkaSslConfigurator.
        }
        // JVM_DEFAULT: nothing to validate.
    }

    private void validateConversion() {
        final String format = trimToNull(getEffectiveConversionFormat());
        if (format == null || "None".equalsIgnoreCase(format)) return;
        if ("PROTOBUF".equalsIgnoreCase(format)) {
            throw new IllegalArgumentException("[SDIA Kafka] PROTOBUF conversion is not implemented. Select None, JSON, or XML.");
        }
        if (!"JSON".equalsIgnoreCase(format) && !"XML".equalsIgnoreCase(format)
                && !"JSON_SCHEMA".equalsIgnoreCase(format)) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Unsupported conversionFormat '" + format + "'. Use None, JSON, or XML.");
        }
        if ("JSON_SCHEMA".equalsIgnoreCase(format)) return;
        if (trimToNull(schemaRegistryHostAddress) == null) {
            throw new IllegalArgumentException(
                    "[SDIA Kafka] Conversion '" + format + "' requires 'schemaRegistryHostAddress'.");
        }
        if (!isSchemaIdFromMagicByte()) {
            final String fixedId = trimToNull(schemaRegistrySchemaId);
            if (fixedId == null) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] 'Fixed Schema ID' mode requires 'schemaRegistrySchemaId'.");
            }
            try {
                if (Integer.parseInt(fixedId) <= 0) throw new NumberFormatException("non-positive");
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException(
                        "[SDIA Kafka] 'schemaRegistrySchemaId' must be a positive numeric value, got: " + fixedId);
            }
        }
        getMaxPayloadSizeBytes();
        getSchemaCacheMaxBytes();
    }

    // =========================================================================
    // Authentication helpers (internal — kept private)
    // =========================================================================

    /**
     * Returns {@code true} for NONE/PLAINTEXT authentication values.
     * This is the internal variant; external callers use {@link #isAuthenticationSaslEffective()}.
     */
    private boolean isPlainTextAuthenticationInternal(final String auth) {
        final String n = normalizeAuthenticationToken(auth);
        if (n == null) return true; // null/empty → treat as PLAINTEXT (safe default)
        return "none".equals(n) || "plaintext".equals(n) || "plain text".equals(n)
                || "none plaintext".equals(n) || "none plain text".equals(n)
                || (n.contains("none") && n.contains("plain"));
    }

    private String normalizeAuthenticationToken(final String value) {
        if (value == null) return null;
        final String raw = value.trim();
        if (raw.isEmpty()) return null;
        final StringBuilder sb = new StringBuilder(raw.length());
        boolean lastWasSpace = true;
        for (int i = 0; i < raw.length(); i++) {
            final char c = raw.charAt(i);
            if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')) {
                sb.append(c >= 'A' && c <= 'Z' ? (char)(c + 32) : c);
                lastWasSpace = false;
            } else if (!lastWasSpace) {
                sb.append(' ');
                lastWasSpace = true;
            }
        }
        final int len = sb.length();
        if (len > 0 && sb.charAt(len - 1) == ' ') sb.setLength(len - 1);
        final String n = sb.toString();
        return n.isEmpty() ? null : n;
    }

    private static String trimToNull(final String value) {
        if (value == null) return null;
        final String t = value.trim();
        return t.isEmpty() ? null : t;
    }

    // =========================================================================
    // Computed size getters
    // =========================================================================

    public Integer getMinFetchSizeBytes() {
        final int mb = normalizeMinFetchSizeMb(minFetchSize);
        return mb <= 0 ? 1 : megabytesToBytes(mb);
    }

    public Integer getMaxFetchSizeBytes() {
        return megabytesToBytes(normalizeFetchSizeMb(maxFetchSize, 5));
    }

    public Integer getMaxPartitionFetchSizeBytes() { return getMaxFetchSizeBytes(); }

    public long getMaxPayloadSizeBytes() {
        return (long) normalizePayloadMb(maxPayloadSizeMb) * 1024L * 1024L;
    }

    public int getSchemaCacheMaxBytes() {
        return normalizeSchemaCacheKb(schemaIdSizeBuffer) * 1024;
    }

    /**
     * Returns the schema cache TTL in milliseconds.
     * Maps the channel dropdown (1h or 2h) to the ms value consumed by
     * {@code SdiaSchemaRegistryClient}.
     */
    public long getSchemaCacheTtlMs() {
        if (schemaCacheTtlH != null && schemaCacheTtlH.intValue() == 2) {
            return 7_200_000L; // 2 hours
        }
        return 3_600_000L; // 1 hour (default)
    }

    public int getEffectiveSchemaIdSizeBuffer() { return 16; }

    // =========================================================================
    // Timestamp parsing (recovery seek)
    // =========================================================================

    public long getSeekToTimestampMs() {
        if (isMessageRecoveryEnabled()) {
            return parseRecoveryTimestampToMillis(recoveryTimestampValue, recoveryTimestampFormat);
        }
        return parseRecoveryTimestampToMillis(seekToTimestamp, "Unix Epoch milliseconds");
    }

    private long parseRecoveryTimestampToMillis(final String rawValue, final String rawFormat) {
        if (rawValue == null || rawValue.trim().isEmpty()) return -1L;
        final String value  = rawValue.trim();
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
            if (value.contains("T") || value.endsWith("Z")) {
                return java.time.Instant.parse(value).toEpochMilli();
            }
            final String digitsOnly = value.startsWith("-") ? value.substring(1) : value;
            if (!DIGITS_PATTERN.matcher(digitsOnly).matches()) return -1L;
            final long parsed = Long.parseLong(value);
            return digitsOnly.length() <= 10 ? parsed * 1000L : parsed;
        } catch (final Throwable ignored) {
            return -1L;
        }
    }

    public boolean isMessageRecoveryEnabled() {
        return recoveryMode != null && "Seek by Timestamp".equalsIgnoreCase(recoveryMode.trim());
    }

    public boolean isRecoverySingleMessageOnly() {
        return recoveryReadMode == null
                || recoveryReadMode.trim().isEmpty()
                || "Read only first matched message".equalsIgnoreCase(recoveryReadMode.trim())
                || "Single Message Only".equalsIgnoreCase(recoveryReadMode.trim())
                || "Read only first matched record".equalsIgnoreCase(recoveryReadMode.trim());
    }

    // =========================================================================
    // Normalisation helpers
    // =========================================================================

    private static int normalizePayloadMb(final Integer value) {
        if (value == null || value < 1) return 1;
        if (value > 5) return 5;
        return value;
    }

    /**
     * Valid schema JSON cache size values (KB): 1–100.
     * Accepted named values from the metadata dropdown: 1, 50, 75, 100.
     * Any value outside [1, 100] is clamped — 200 KB is NOT supported because
     * the Kafka record value size and SAP CPI heap constraints make schemas
     * larger than 100 KB impractical for in-memory CPI processing.
     * The previous metadata.xml had 200 KB as an option; that was a documentation
     * bug — the code has always clamped at 100 KB. The 200 KB option is removed.
     */
    private static int normalizeSchemaCacheKb(final Integer value) {
        if (value == null || value < 1) return 50;
        if (value > 100) return 100;  // Hard ceiling — 200 KB option removed from metadata
        return value;
    }

    private static int normalizeMinFetchSizeMb(final Integer value) {
        long raw = value == null ? 0L : value.longValue();
        if (raw > 1024L) raw = (raw + 1_048_575L) / 1_048_576L;
        if (raw < 0L) return 0;
        if (raw > 20L) return 20;
        return (int) raw;
    }

    private static int normalizeFetchSizeMb(final Integer value, final int defaultMb) {
        long raw = value == null ? defaultMb : value.longValue();
        if (raw <= 0L) raw = defaultMb;
        if (raw > 1024L) raw = (raw + 1_048_575L) / 1_048_576L;
        if (raw < 1L) return 1;
        if (raw > 20L) return 20;
        return (int) raw;
    }

    private static int normalizeFetchWaitMs(final Integer value) {
        final int ms = value == null ? 500 : value.intValue();
        if (ms < 50)   return 50;
        if (ms > 1000) return 500;
        return ms;
    }

    private static int megabytesToBytes(final int mb) { return mb * 1024 * 1024; }

    private static Integer parseInteger(final String value) {
        if (value == null) return null;
        try { final String t = value.trim(); return t.isEmpty() ? null : Integer.valueOf(t); }
        catch (final NumberFormatException e) { return null; }
    }

    private static Long parseLong(final String value) {
        if (value == null) return null;
        try { final String t = value.trim(); return t.isEmpty() ? null : Long.valueOf(t); }
        catch (final NumberFormatException e) { return null; }
    }

    private static int  positiveOrDefault(final Integer v, final int  d) { return v != null && v > 0 ? v : d; }
    private static long positiveOrDefault(final Long    v, final long d) { return v != null && v > 0L ? v : d; }

    // =========================================================================
    // Getters & setters
    // =========================================================================

    public String  getFirstUriPart()               { return firstUriPart; }
    public void    setFirstUriPart(String v)       { firstUriPart = v; }

    public String  getProxyType()                  { return proxyType; }
    public void    setProxyType(String v)          { proxyType = (v == null || v.isEmpty()) ? "Internet" : v; }

    public String  getSapSapccLocationId()         { return sapSapccLocationId; }
    public void    setSapSapccLocationId(String v) { sapSapccLocationId = v; }

    public String  getAuthentication()             { return authentication; }
    public void    setAuthentication(String v)     { authentication = v; }

    public Boolean getConnectWithTls()             { return connectWithTls; }
    public void    setConnectWithTls(Boolean v)    { connectWithTls = v != null ? v : Boolean.TRUE; }
    public void    setConnectWithTls(String v)     {
        // ADK binds xsd:string FixedValues "true"/"false" as a String.
        connectWithTls = !"false".equalsIgnoreCase(v == null ? null : v.trim());
    }

    // ── New TLS trust source fields ──────────────────────────────────────────

    public String  getTlsTrustSource()             { return tlsTrustSource; }
    public void    setTlsTrustSource(String v) {
        tlsTrustSource = (v == null || v.trim().isEmpty()) ? "JVM_DEFAULT" : v.trim();
    }

    public String  getTlsTrustedCaPem()            { return tlsTrustedCaPem; }
    public void    setTlsTrustedCaPem(String v)    { tlsTrustedCaPem = v; }

    public String  getTlsTrustedCertificateAlias() { return tlsTrustedCertificateAlias; }
    public void    setTlsTrustedCertificateAlias(String v) { tlsTrustedCertificateAlias = v; }

    // ── Legacy trust fields (backward compat) ────────────────────────────────

    public String  getCertificateAlias()           { return certificateAlias; }
    public void    setCertificateAlias(String v)   { certificateAlias = v; }

    public String  getBrokerCaSource()             { return brokerCaSource; }
    public void    setBrokerCaSource(String v)     { brokerCaSource = (v == null || v.isEmpty()) ? "None" : v; }

    // ── SASL ─────────────────────────────────────────────────────────────────

    public String  getSaslMechanism()              { return saslMechanism; }
    public void    setSaslMechanism(String v)      { saslMechanism = v; }

    public String  getCredentialAlias()            { return credentialAlias; }
    public void    setCredentialAlias(String v)    { credentialAlias = v; }

    // ── mTLS / OAuth (reserved) ──────────────────────────────────────────────

    public String  getMtlsKeyAlias()               { return mtlsKeyAlias; }
    public void    setMtlsKeyAlias(String v)       { mtlsKeyAlias = v; }

    public String  getOauthTokenEndpoint()         { return oauthTokenEndpoint; }
    public void    setOauthTokenEndpoint(String v) { oauthTokenEndpoint = v; }

    public String  getOauthClientId()              { return oauthClientId; }
    public void    setOauthClientId(String v)      { oauthClientId = v; }

    public String  getOauthClientSecretAlias()     { return oauthClientSecretAlias; }
    public void    setOauthClientSecretAlias(String v) { oauthClientSecretAlias = v; }

    public String  getOauthScope()                 { return oauthScope; }
    public void    setOauthScope(String v)         { oauthScope = v; }

    // ── Topic / consumer ─────────────────────────────────────────────────────

    public String  getTopicPattern()               { return topicPattern; }
    public void    setTopicPattern(String v)       { topicPattern = v; }

    public String  getPartitions()                 { return partitions; }
    public void    setPartitions(String v)         { partitions = v; }

    public Integer getParallelConsumers()          { return parallelConsumers; }
    public void    setParallelConsumers(Integer v) { parallelConsumers = v != null ? v : 1; }

    public Integer getMinFetchSize()               { return minFetchSize; }
    public void    setMinFetchSize(Integer v)      { minFetchSize = normalizeMinFetchSizeMb(v); }
    public void    setMinFetchSize(String v)       { minFetchSize = normalizeMinFetchSizeMb(parseInteger(v)); }

    public Integer getMaxFetchSize()               { return maxFetchSize; }
    public void    setMaxFetchSize(Integer v)      { maxFetchSize = normalizeFetchSizeMb(v, 5); }
    public void    setMaxFetchSize(String v)       { maxFetchSize = normalizeFetchSizeMb(parseInteger(v), 5); }

    public Integer getMaxPartitionFetchSize()      { return maxPartitionFetchSize; }
    public void    setMaxPartitionFetchSize(Integer v) { maxPartitionFetchSize = normalizeFetchSizeMb(v, 5); }
    public void    setMaxPartitionFetchSize(String v)  { maxPartitionFetchSize = normalizeFetchSizeMb(parseInteger(v), 5); }

    public Integer getMaxFetchWaitTime()           { return normalizeFetchWaitMs(maxFetchWaitTime); }
    public void    setMaxFetchWaitTime(Integer v)  { maxFetchWaitTime = normalizeFetchWaitMs(v); }
    public void    setMaxFetchWaitTime(String v)   { maxFetchWaitTime = normalizeFetchWaitMs(parseInteger(v)); }

    public Integer getMaxRetryBackoffMs()          { return maxRetryBackoffMs; }
    public void    setMaxRetryBackoffMs(Integer v) { maxRetryBackoffMs = positiveOrDefault(v, 1000); }
    public void    setMaxRetryBackoffMs(String v)  { maxRetryBackoffMs = positiveOrDefault(parseInteger(v), 1000); }

    public Long    getReconnectDelayS()            { return reconnectDelayS; }
    public void    setReconnectDelayS(Long v)      { reconnectDelayS = positiveOrDefault(v, 1L); }
    public void    setReconnectDelayS(String v)    { reconnectDelayS = positiveOrDefault(parseLong(v), 1L); }

    public Long    getHeartbeatIntervalS()         { return heartbeatIntervalS; }
    public void    setHeartbeatIntervalS(Long v)   { heartbeatIntervalS = positiveOrDefault(v, 3L); }
    public void    setHeartbeatIntervalS(String v) { heartbeatIntervalS = positiveOrDefault(parseLong(v), 3L); }

    public Long    getSessionTimeoutS()            { return sessionTimeoutS; }
    public void    setSessionTimeoutS(Long v)      { sessionTimeoutS = positiveOrDefault(v, 10L); }
    public void    setSessionTimeoutS(String v)    { sessionTimeoutS = positiveOrDefault(parseLong(v), 10L); }

    public Integer getMaxPollIntervalMs()          { return maxPollIntervalMs; }
    public void    setMaxPollIntervalMs(Integer v) {
        int val = positiveOrDefault(v, 60000);
        if (val < 10000) val = 10000;
        if (val > 60000) val = 60000;
        maxPollIntervalMs = val;
    }
    public void    setMaxPollIntervalMs(String v)  { setMaxPollIntervalMs(parseInteger(v)); }

    public String  getErrorHandling()              { return errorHandling; }
    public void    setErrorHandling(String v)      { errorHandling = (v == null || v.isEmpty()) ? "Stop on Error" : v; }

    public Integer getRetryAttempts()              { return retryAttempts; }
    public void    setRetryAttempts(Integer v)     { retryAttempts = (v != null && v >= 5 && v <= 10) ? v : 5; }
    public void    setRetryAttempts(String v) {
        if (v == null) { retryAttempts = 5; return; }
        try { setRetryAttempts(Integer.valueOf(v.trim())); } catch (NumberFormatException ignored) { retryAttempts = 5; }
    }

    public String  getConversionFormat()           { return conversionFormat; }
    public void    setConversionFormat(String v)   { conversionFormat = (v == null || v.isEmpty()) ? "None" : v; }

    public String  getMessageConversion()          { return messageConversion; }
    public void    setMessageConversion(String v)  { messageConversion = (v == null || v.isEmpty()) ? "RAW_BYTES" : v; }

    public String  getProducerRegistryProfile()    { return producerRegistryProfile; }
    public void    setProducerRegistryProfile(String v) { producerRegistryProfile = (v == null || v.isEmpty()) ? "CONFLUENT_CLOUD" : v; }

    public String  getPayloadEncoding()            { return payloadEncoding; }
    public void    setPayloadEncoding(String v)    { payloadEncoding = (v == null || v.isEmpty()) ? "AVRO_SCHEMA_REGISTRY_WIRE" : v; }

    public String  getAvroOutputFormat()           { return avroOutputFormat; }
    public void    setAvroOutputFormat(String v)   { avroOutputFormat = (v == null || v.isEmpty()) ? "JSON" : v; }

    public String  getSchemaRegistryType()         { return schemaRegistryType; }
    public void    setSchemaRegistryType(String v) { schemaRegistryType = (v == null || v.isEmpty()) ? "CONFLUENT_KARAPACE" : v; }

    public String  getSchemaIdResolutionMode()     { return schemaIdResolutionMode; }
    public void    setSchemaIdResolutionMode(String v) { schemaIdResolutionMode = (v == null || v.isEmpty()) ? "Magic Byte" : v.trim(); }

    public String  getSchemaRegistryHostAddress()          { return schemaRegistryHostAddress; }
    public void    setSchemaRegistryHostAddress(String v)  { schemaRegistryHostAddress = v; }

    public String  getSchemaRegistryCredentialAlias()      { return schemaRegistryCredentialAlias; }
    public void    setSchemaRegistryCredentialAlias(String v) { schemaRegistryCredentialAlias = v; }

    public String  getSchemaRegistrySchemaId()             { return schemaRegistrySchemaId; }
    public void    setSchemaRegistrySchemaId(String v)     { schemaRegistrySchemaId = v; }

    public String  getMaxPayloadConversionSize() { return maxPayloadConversionSize; }
    public void    setMaxPayloadConversionSize(String v) {
        final int mb = normalizePayloadMb(parseInteger(v));
        maxPayloadSizeMb = mb;
        maxPayloadConversionSize = String.valueOf(mb);
    }

    public String  getSchemaIdBufferSize() { return schemaIdBufferSize; }
    public void    setSchemaIdBufferSize(String v) {
        final int kb = normalizeSchemaCacheKb(parseInteger(v));
        schemaIdSizeBuffer = kb;
        schemaIdBufferSize = String.valueOf(kb);
    }

    public Integer getMaxPayloadSizeMb()           { return maxPayloadSizeMb; }
    public void    setMaxPayloadSizeMb(Integer v)  {
        final int mb = normalizePayloadMb(v);
        maxPayloadSizeMb = mb;
        maxPayloadConversionSize = String.valueOf(mb);
    }
    public void    setMaxPayloadSizeMb(String v)   { setMaxPayloadConversionSize(v); }

    public Integer getSchemaIdSizeBuffer()         { return schemaIdSizeBuffer; }
    public void    setSchemaIdSizeBuffer(Integer v) {
        final int kb = normalizeSchemaCacheKb(v);
        schemaIdSizeBuffer = kb;
        schemaIdBufferSize = String.valueOf(kb);
    }
    public void    setSchemaIdSizeBuffer(String v) { setSchemaIdBufferSize(v); }

    public Integer getSchemaCacheTtlH()            { return schemaCacheTtlH; }
    public void    setSchemaCacheTtlH(Integer v)   {
        schemaCacheTtlH = (v != null && v.intValue() == 2) ? 2 : 1;
    }
    public void    setSchemaCacheTtlH(String v) {
        if (v == null) { schemaCacheTtlH = 1; return; }
        try { setSchemaCacheTtlH(Integer.valueOf(v.trim())); }
        catch (NumberFormatException ignored) { schemaCacheTtlH = 1; }
    }

    public Long    getPollTimeoutS()               { return pollTimeoutS; }
    public void    setPollTimeoutS(Long v) {
        pollTimeoutS  = (v == null || v < 1L) ? 1L : v;
        pollTimeoutMs = pollTimeoutS * 1000L;
    }

    public Long    getPollTimeoutMs()              { return pollTimeoutMs; }
    public void    setPollTimeoutMs(Long v) {
        pollTimeoutMs = (v == null || v < 1L) ? 1000L : v;
        pollTimeoutS  = Math.max(1L, pollTimeoutMs / 1000L);
    }

    public Long    getInitialDelayS()              { return initialDelayS; }
    public void    setInitialDelayS(Long v)        { initialDelayS = 0L; super.setInitialDelay(0L); }

    public Long    getDelayS()                     { return delayS; }
    public void    setDelayS(Long v)               { delayS = 1L; super.setDelay(1000L); }

    public Integer getMaxPollRecords()             { return maxPollRecords; }
    public void    setMaxPollRecords(Integer v)    { maxPollRecords = v != null ? v : 10; }

    public String  getGroupId()                    { return groupId; }
    public void    setGroupId(String v)            { groupId = v; }

    public String  getAutoOffsetReset()            { return autoOffsetReset; }
    public void    setAutoOffsetReset(String v)    { autoOffsetReset = v; }

    public String  getTargetTopic()                { return targetTopic; }
    public void    setTargetTopic(String v)        { targetTopic = v; }

    public String  getKeyHeaderName()              { return keyHeaderName; }
    public void    setKeyHeaderName(String v)      { keyHeaderName = v; }

    public String  getAcks()                       { return acks; }
    public void    setAcks(String v)               { acks = v; }

    public Long    getRequestTimeoutS()            { return requestTimeoutS; }
    public void    setRequestTimeoutS(Long v) {
        requestTimeoutS  = (v == null || v < 1L) ? 30L : v;
        requestTimeoutMs = requestTimeoutS * 1000L;
    }

    public Long    getRequestTimeoutMs()           { return requestTimeoutMs; }
    public void    setRequestTimeoutMs(Long v) {
        requestTimeoutMs = (v == null || v < 1L) ? 30000L : v;
        requestTimeoutS  = Math.max(1L, requestTimeoutMs / 1000L);
    }

    public Long    getDeliveryTimeoutS()           { return deliveryTimeoutS; }
    public void    setDeliveryTimeoutS(Long v) {
        deliveryTimeoutS  = (v == null || v < 1L) ? 120L : v;
        deliveryTimeoutMs = deliveryTimeoutS * 1000L;
    }

    public Long    getDeliveryTimeoutMs()          { return deliveryTimeoutMs; }
    public void    setDeliveryTimeoutMs(Long v) {
        deliveryTimeoutMs = (v == null || v < 1L) ? 120000L : v;
        deliveryTimeoutS  = Math.max(1L, deliveryTimeoutMs / 1000L);
    }

    public String  getKafkaClusterHostsTable()     { return kafkaClusterHostsTable; }
    public void    setKafkaClusterHostsTable(String v) { kafkaClusterHostsTable = v; }

    public String  getKafkaHostRow()               { return kafkaHostRow; }
    public void    setKafkaHostRow(String v)       { kafkaHostRow = v; }

    public String  getKafkaPortRow()               { return kafkaPortRow; }
    public void    setKafkaPortRow(String v)       { kafkaPortRow = v; }

    public String  getSeekToTimestamp()            { return seekToTimestamp; }
    public void    setSeekToTimestamp(String v)    { seekToTimestamp = (v != null && v.trim().isEmpty()) ? null : v; }

    public String  getRecoveryMode()               { return recoveryMode; }
    public void    setRecoveryMode(String v)       { recoveryMode = (v != null && !v.trim().isEmpty()) ? v.trim() : "Disabled"; }

    public String  getRecoveryTimestampFormat()    { return recoveryTimestampFormat; }
    public void    setRecoveryTimestampFormat(String v) { recoveryTimestampFormat = (v != null && !v.trim().isEmpty()) ? v.trim() : "Auto Detect"; }

    public String  getRecoveryTimestampValue()     { return recoveryTimestampValue; }
    public void    setRecoveryTimestampValue(String v) { recoveryTimestampValue = (v != null && !v.trim().isEmpty()) ? v.trim() : null; }

    public String  getRecoveryReadMode()           { return recoveryReadMode; }
    public void    setRecoveryReadMode(String v)   { recoveryReadMode = (v != null && !v.trim().isEmpty()) ? v.trim() : "Read only first matched message"; }
}
