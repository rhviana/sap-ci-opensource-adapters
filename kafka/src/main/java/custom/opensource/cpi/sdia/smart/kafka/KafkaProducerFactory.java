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
 *   Apache License, Version 2.0
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 *   MIT License
 *   https://opensource.org/licenses/MIT
 *
 * You may use this software under either license at your option.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under these licenses is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * ----------------------------------------------------------------------------
 *
 * ⚠️  NOTICE — This header must NOT be removed or altered in any distribution,
 *     derivative work, or reuse of this source code, in whole or in part.
 *     Removal of this header constitutes a violation of the license terms
 *     and applicable intellectual property rights.
 *
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Locale;
import java.util.Properties;

/**
 * Kafka producer properties factory for the EventSmartKafka adapter.
 *
 * <h3>Security protocol matrix (v1.0.1)</h3>
 * Identical to {@link KafkaConsumerFactory} — derived from two orthogonal booleans:
 * <pre>
 * Authentication=NONE  + TLS=false  →  PLAINTEXT
 * Authentication=NONE  + TLS=true   →  SSL
 * Authentication=SASL  + TLS=false  →  SASL_PLAINTEXT
 * Authentication=SASL  + TLS=true   →  SASL_SSL
 * </pre>
 *
 * SSL trust material is applied via {@link SdiaKafkaSslConfigurator#configure(Properties, SdiaKafkaEndpoint)}.
 * The old {@code applySslMaterial(endpoint, props, boolean)} method no longer exists.
 *
 * <h3>Scope (v1.0)</h3>
 * OAuth 2.0 and mTLS are not implemented. Supported SASL mechanisms:
 * PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
 */
public class KafkaProducerFactory {

    public Properties createProperties(final SdiaKafkaEndpoint endpoint) {
        final Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,      endpoint.resolveBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.ACKS_CONFIG,                   endpoint.getAcks());
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,     String.valueOf(endpoint.getRequestTimeoutMs()));
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,    String.valueOf(endpoint.getDeliveryTimeoutMs()));

        applySecurity(endpoint, props);

        return props;
    }

    // -------------------------------------------------------------------------
    // Security — same 4-cell matrix as KafkaConsumerFactory
    // -------------------------------------------------------------------------

    private void applySecurity(final SdiaKafkaEndpoint endpoint, final Properties props) {
        final boolean tls  = endpoint.isTlsEnabledEffective();
        final boolean sasl = endpoint.isAuthenticationSaslEffective();

        // ── Step 1: security.protocol ─────────────────────────────────────────
        final String securityProtocol;
        if      (sasl && tls)  { securityProtocol = "SASL_SSL";       }
        else if (sasl)          { securityProtocol = "SASL_PLAINTEXT"; }
        else if (tls)           { securityProtocol = "SSL";            }
        else                    { securityProtocol = "PLAINTEXT";      }
        props.put("security.protocol", securityProtocol);

        // ── Step 2: SASL (only when sasl == true) ─────────────────────────────
        if (sasl) {
            final String mechanism = normalizeMechanism(endpoint.getSaslMechanism());
            props.put("sasl.mechanism", mechanism);
            props.put("sasl.login.class", "org.apache.kafka.common.security.authenticator.DefaultLogin");
            final SdiaKafkaCredentials credentials =
                    SdiaKafkaCredentialsResolver.resolve(endpoint.getCredentialAlias());
            props.put("sasl.jaas.config", buildJaasConfig(mechanism, credentials));
        } else {
            props.remove("sasl.mechanism");
            props.remove("sasl.jaas.config");
        }

        // ── Step 3: TLS trust (only when tls == true) ─────────────────────────
        if (tls) {
            SdiaKafkaSslConfigurator.configure(props, endpoint);
        }
    }

    // -------------------------------------------------------------------------
    // SASL helpers
    // -------------------------------------------------------------------------

    private String normalizeMechanism(final String mechanism) {
        if (mechanism == null || mechanism.trim().isEmpty()) {
            return "PLAIN";
        }
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

    private String buildJaasConfig(final String mechanism,
                                    final SdiaKafkaCredentials credentials) {
        final boolean isPlain = "PLAIN".equals(mechanism);
        final String loginModule = isPlain
                ? "org.apache.kafka.common.security.plain.PlainLoginModule"
                : "org.apache.kafka.common.security.scram.ScramLoginModule";

        final StringBuilder jaas = new StringBuilder(256);
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
     * Escapes {@code \} and {@code "} in-place without intermediate String objects.
     */
    private static void escapeToBuffer(final StringBuilder buffer, final String value) {
        if (value == null) { return; }
        for (int i = 0, len = value.length(); i < len; i++) {
            final char c = value.charAt(i);
            if      (c == '\\') { buffer.append("\\\\"); }
            else if (c == '"')  { buffer.append("\\\""); }
            else                { buffer.append(c); }
        }
    }
}
