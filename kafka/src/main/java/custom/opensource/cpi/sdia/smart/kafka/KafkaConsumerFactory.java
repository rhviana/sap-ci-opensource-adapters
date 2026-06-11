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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Kafka consumer properties factory for the EventSmartKafka adapter.
 *
 * <h3>Security protocol matrix (v1.0.1)</h3>
 * <pre>
 * Authentication=NONE  + TLS=false  →  PLAINTEXT
 *   • No credential lookup. No SASL. No SSL properties.
 *
 * Authentication=NONE  + TLS=true   →  SSL
 *   • No credential lookup. No SASL.
 *   • SSL trust properties set by SdiaKafkaSslConfigurator.
 *
 * Authentication=SASL  + TLS=false  →  SASL_PLAINTEXT
 *   • Credential resolved from SAP CPI Secure Store.
 *   • SASL mechanism + JAAS config set.
 *   • No SSL properties.
 *
 * Authentication=SASL  + TLS=true   →  SASL_SSL
 *   • Credential resolved from SAP CPI Secure Store.
 *   • SASL mechanism + JAAS config set.
 *   • SSL trust properties set by SdiaKafkaSslConfigurator.
 * </pre>
 *
 * <h3>Authentication scope (v1.0)</h3>
 * OAuth 2.0, mTLS, and Client Certificate profiles are removed from v1.0.
 * Supported: NONE/PLAINTEXT, SASL/PLAIN, SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512.
 *
 * <h3>GC / hotpath notes</h3>
 * Pre-sized {@link StringBuilder} instances, single-pass ASCII normalisation,
 * insertion sort for small topic lists, and direct ASCII arithmetic for
 * upper-casing eliminate per-call allocations on the configuration path.
 */
public final class KafkaConsumerFactory {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFactory.class);

    private static final String CONSUMER_GROUP_PREFIX = "EventSmartKafka-";
    private static final int    MAX_GROUP_ID_LENGTH   = 80;

    // =========================================================================
    // Public API
    // =========================================================================

    /**
     * Builds Kafka consumer {@link Properties} for the given adapter endpoint.
     *
     * <p>Security properties are applied last so they always win over any
     * previously set defaults. This method is stateless and thread-safe.
     *
     * @param endpoint non-null endpoint carrying adapter channel parameters
     * @return fully populated Kafka consumer properties
     */
    public Properties createProperties(final SdiaKafkaEndpoint endpoint) {
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,    endpoint.resolveBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG,             resolveConsumerGroupId(endpoint));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,    endpoint.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,   "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,     String.valueOf(endpoint.getMaxPollRecords()));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        // Security is applied after all other properties so it is always authoritative.
        applySecurity(endpoint, props);

        return props;
    }

    // =========================================================================
    // Group ID derivation
    // =========================================================================

    private String resolveConsumerGroupId(final SdiaKafkaEndpoint endpoint) {
        final String configured = trimToNull(endpoint.getGroupId());
        if (configured != null) {
            return configured;
        }
        final String host  = trimToNull(endpoint.resolveBootstrapServers());
        final String topic = normalizeTopicExpressionForGroupId(endpoint.getTopicPattern());
        final int hostLen  = host  != null ? host.length()  : 4;
        final int topicLen = topic != null ? topic.length() : 4;
        final StringBuilder seed = new StringBuilder(hostLen + 1 + topicLen);
        seed.append(host  != null ? host  : "null");
        seed.append('|');
        seed.append(topic != null ? topic : "null");
        final String hash     = Integer.toHexString(seed.toString().hashCode());
        final String baseName = (topic != null) ? topic : "consumer";
        return sanitizeAndBuildGroupId(baseName, hash);
    }

    private String normalizeTopicExpressionForGroupId(final String value) {
        final String expression = trimToNull(value);
        if (expression == null) { return null; }
        final List<String> entries = new ArrayList<>(4);
        int start = 0;
        final int len = expression.length();
        while (start < len) {
            int comma = expression.indexOf(',', start);
            if (comma == -1) { comma = len; }
            final String entry = trimToNull(expression.substring(start, comma));
            if (entry != null && !entries.contains(entry)) { entries.add(entry); }
            start = comma + 1;
        }
        final int size = entries.size();
        if (size == 0) { return null; }
        for (int i = 1; i < size; i++) {
            final String key = entries.get(i);
            int j = i - 1;
            while (j >= 0 && entries.get(j).compareTo(key) > 0) {
                entries.set(j + 1, entries.get(j));
                j--;
            }
            entries.set(j + 1, key);
        }
        final StringBuilder canonical = new StringBuilder(128);
        for (int i = 0; i < size; i++) {
            if (i > 0) { canonical.append(','); }
            canonical.append(entries.get(i));
        }
        return canonical.toString();
    }

    private String sanitizeAndBuildGroupId(final String topic, final String hash) {
        final StringBuilder out = new StringBuilder(
                CONSUMER_GROUP_PREFIX.length() + topic.length() + 1 + hash.length());
        out.append(CONSUMER_GROUP_PREFIX);
        boolean lastWasDash = false;
        final int topicLen = topic.length();
        for (int i = 0; i < topicLen; i++) {
            final char c = topic.charAt(i);
            final boolean valid = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
                    || (c >= '0' && c <= '9') || c == '.' || c == '_' || c == '-';
            if (valid) {
                if (c == '-') {
                    if (!lastWasDash) { out.append('-'); lastWasDash = true; }
                } else {
                    out.append(c); lastWasDash = false;
                }
            } else {
                if (!lastWasDash) { out.append('-'); lastWasDash = true; }
            }
        }
        out.append('-').append(hash);
        if (out.length() > MAX_GROUP_ID_LENGTH) { out.setLength(MAX_GROUP_ID_LENGTH); }
        return out.toString();
    }

    // =========================================================================
    // Security dispatch — the 4-cell protocol matrix
    // =========================================================================

    /**
     * Applies the correct Kafka security properties using the authoritative
     * 4-cell protocol matrix.
     *
     * <h3>Credential guard</h3>
     * {@link SdiaKafkaCredentialsResolver#resolve(String)} is called ONLY when
     * {@code sasl == true}. For {@code NONE} + any TLS state, no credential
     * lookup is performed — preventing "alias not found" errors and the metadata
     * timeout that occurred on {@code sdia-kafka:19092}.
     *
     * <h3>SSL configurator guard</h3>
     * {@link SdiaKafkaSslConfigurator#configure(Properties, SdiaKafkaEndpoint)}
     * is called ONLY when {@code tls == true}, ensuring that no SSL/truststore
     * properties are ever set for {@code PLAINTEXT} or {@code SASL_PLAINTEXT}.
     */
    private void applySecurity(final SdiaKafkaEndpoint endpoint, final Properties props) {
        final boolean tls  = endpoint.isTlsEnabledEffective();
        final boolean sasl = endpoint.isAuthenticationSaslEffective();

        // ── Step 1: derive and set security.protocol ─────────────────────────
        final String securityProtocol;
        if (sasl && tls) {
            securityProtocol = "SASL_SSL";
        } else if (sasl) {
            securityProtocol = "SASL_PLAINTEXT";
        } else if (tls) {
            securityProtocol = "SSL";
        } else {
            securityProtocol = "PLAINTEXT";
        }
        props.put("security.protocol", securityProtocol);

        // ── Step 2: SASL credential + JAAS (only when sasl == true) ──────────
        if (sasl) {
            final String mechanism = normalizeSaslMechanism(endpoint.getSaslMechanism());
            props.put("sasl.mechanism", mechanism);
            // v1.0 supports only PLAIN/SCRAM. DefaultLogin skips the Kerberos login
            // path whose class-init references org.ietf.jgss.GSSException — a package
            // the CPI OSGi container does not export (stubs embedded as belt-and-suspenders).
            props.put("sasl.login.class", "org.apache.kafka.common.security.authenticator.DefaultLogin");

            final SdiaKafkaCredentials credentials =
                    SdiaKafkaCredentialsResolver.resolve(endpoint.getCredentialAlias());
            props.put("sasl.jaas.config", buildJaasConfig(mechanism, credentials));
        } else {
            // Explicitly remove any SASL keys that may have been set by a
            // previous Properties merge. NONE mode must be completely SASL-free.
            props.remove("sasl.mechanism");
            props.remove("sasl.jaas.config");
            props.remove("sasl.login.callback.handler.class");
        }

        // ── Step 3: TLS trust (only when tls == true) ─────────────────────────
        if (tls) {
            SdiaKafkaSslConfigurator.configure(props, endpoint);
        }

        // ── Diagnostic log (no secrets logged) ──────────────────────────────
        // LOG.warn, not LOG.debug: security profile must be visible in production
        // CPI logs without enabling debug mode. SLF4J {} bindings are lazy by
        // design — isDebugEnabled() guard is only needed when arguments require
        // manual string concatenation before the LOG call; it is redundant here.
        LOG.warn("[SDIA Kafka][KafkaConsumerFactory] Security profile applied."
                + " authentication=" + endpoint.getAuthentication()
                + " tls=" + tls
                + " sasl=" + sasl
                + " securityProtocol=" + securityProtocol
                + " saslMechanism=" + (sasl ? endpoint.getSaslMechanism() : "n/a")
                + " trustSource=" + (tls ? endpoint.getTlsTrustSourceEffective() : "n/a")
                + " credentialAlias=" + (sasl ? (trimToNull(endpoint.getCredentialAlias()) != null ? "configured" : "<empty>") : "n/a"));
    }

    // =========================================================================
    // SASL JAAS configuration
    // =========================================================================

    /**
     * Normalises the SASL mechanism string to its canonical upper-case form.
     * Only PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 are supported in v1.0.
     */
    private String normalizeSaslMechanism(final String mechanism) {
        if (mechanism == null || mechanism.trim().isEmpty()) { return "PLAIN"; }
        final String trimmed = mechanism.trim();
        final int len = trimmed.length();
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            final char c = trimmed.charAt(i);
            sb.append((c >= 'a' && c <= 'z') ? (char)(c - 32) : c);
        }
        final String normalised = sb.toString();
        if ("PLAIN".equals(normalised)
                || "SCRAM-SHA-256".equals(normalised)
                || "SCRAM-SHA-512".equals(normalised)) {
            return normalised;
        }
        throw new IllegalArgumentException(
                "[SDIA Kafka] Unsupported SASL mechanism: '" + mechanism + "'. "
                + "Supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.");
    }

    /**
     * Builds a Kafka JAAS config string for PLAIN or SCRAM login modules.
     * Escapes {@code \} and {@code "} in-place without intermediate String objects.
     */
    private String buildJaasConfig(final String mechanism,
                                    final SdiaKafkaCredentials credentials) {
        final boolean isPlain   = "PLAIN".equals(mechanism);
        final String loginModule = isPlain
                ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";

        final int usernameLen = credentials.getUsername() != null ? credentials.getUsername().length() : 0;
        final int passwordLen = credentials.getPassword() != null ? credentials.getPassword().length() : 0;
        final StringBuilder jaas = new StringBuilder(loginModule.length() + usernameLen + passwordLen + 80);

        jaas.append(loginModule).append(" required username=\"");
        escapeToBuffer(jaas, credentials.getUsername());
        jaas.append("\" password=\"");
        escapeToBuffer(jaas, credentials.getPassword());
        jaas.append('"');
        if (!isPlain) {
            jaas.append(" algorithm=\"").append(mechanism).append('"');
        }
        jaas.append(';');
        return jaas.toString();
    }

    /**
     * Appends a JAAS-safe-escaped version of {@code value} to {@code buffer}.
     * Only {@code \} and {@code "} require escaping. Writes directly to the
     * caller's buffer — no intermediate String allocation.
     */
    private static void escapeToBuffer(final StringBuilder buffer, final String value) {
        if (value == null) { return; }
        for (int i = 0, len = value.length(); i < len; i++) {
            final char c = value.charAt(i);
            if (c == '\\') {
                buffer.append("\\\\");
            } else if (c == '"') {
                buffer.append("\\\"");
            } else {
                buffer.append(c);
            }
        }
    }

    // =========================================================================
    // Utility
    // =========================================================================

    private static String trimToNull(final String value) {
        if (value == null) { return null; }
        final String t = value.trim();
        return t.isEmpty() ? null : t;
    }
}
