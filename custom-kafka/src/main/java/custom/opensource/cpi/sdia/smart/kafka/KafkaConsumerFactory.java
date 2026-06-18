/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
 * ============================================================================
 *
 * Copyright (c) 2026 Ricardo Luz Holanda Viana
 * Independent Solo Researcher | Enterprise Integration Architecture
 *
 * Dual-Licensed under Apache License 2.0 or MIT License.
 * ============================================================================
 */
package custom.opensource.cpi.sdia.smart.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Kafka consumer factory for CPI/OSGi runtime.
 * Keeps payload values as byte[]; key deserialization remains String for backward-compatible Camel headers.
 */
public class KafkaConsumerFactory {

    private static final String DEFAULT_CONSUMER_PREFIX = "EventSmartKafka-";

    public Properties createProperties(final SdiaKafkaEndpoint endpoint) {
        // Inicializa as propriedades com capacidade fixa para evitar re-hashing interno
        final Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, endpoint.resolveBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, resolveConsumerGroupId(endpoint));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, endpoint.getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(endpoint.getMaxPollRecords()));

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);

        applySecurity(endpoint, props);

        return props;
    }

    private String resolveConsumerGroupId(final SdiaKafkaEndpoint endpoint) {
        final String configured = trimToNull(endpoint.getGroupId());
        if (configured != null) {
            return configured;
        }

        final String host = trimToNull(endpoint.resolveBootstrapServers());
        final String topic = normalizeTopicExpressionForGroupId(endpoint.getTopicPattern());

        // Otimização de concatenação usando StringBuilder dimensionado em vez de operador + implicando múltiplos Builders
        final int hostLen = host != null ? host.length() : 4;
        final int topicLen = topic != null ? topic.length() : 4;
        final StringBuilder seedBuilder = new StringBuilder(hostLen + topicLen + 1);
        seedBuilder.append(host).append('|').append(topic);

        final String hash = Integer.toHexString(seedBuilder.toString().hashCode());
        final String baseTopic = (topic == null) ? "consumer" : topic;

        return sanitizeAndBuildGroupId(baseTopic, hash);
    }

    private String normalizeTopicExpressionForGroupId(final String value) {
        final String expression = trimToNull(value);
        if (expression == null) {
            return null;
        }

        // Parsing linear sem Regex/Split para evitar alocações massivas no Heap
        final List<String> entries = new ArrayList<>(8);
        int start = 0;
        final int len = expression.length();

        while (start < len) {
            int comma = expression.indexOf(',', start);
            if (comma == -1) {
                comma = len;
            }
            final String entry = trimToNull(expression.substring(start, comma));
            if (entry != null && !entries.contains(entry)) {
                entries.add(entry);
            }
            start = comma + 1;
        }

        final int size = entries.size();
        if (size == 0) {
            return null;
        }

        // Insertion Sort in-place otimizado para coleções pequenas (evita overhead do Timsort do Collections.sort)
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
            if (i > 0) {
                canonical.append(',');
            }
            canonical.append(entries.get(i));
        }
        return canonical.toString();
    }

    /**
     * Otimização Máxima: Fusão do algoritmo de sanitização, remoção de duplicidade de traços
     * e truncamento de tamanho em uma única passada de array (Single-pass Buffer Processing).
     */
    private String sanitizeAndBuildGroupId(final String topic, final String hash) {
        final StringBuilder out = new StringBuilder(128);
        out.append(DEFAULT_CONSUMER_PREFIX);

        final int topicLen = topic.length();
        boolean lastWasDash = false;

        // Passada única higienizando caracteres e comprimindo múltiplos hifens '--' nativamente
        for (int i = 0; i < topicLen; i++) {
            final char c = topic.charAt(i);
            final boolean isValid = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9')
                    || c == '.' || c == '_' || c == '-';

            if (isValid) {
                if (c == '-') {
                    if (!lastWasDash) {
                        out.append('-');
                        lastWasDash = true;
                    }
                } else {
                    out.append(c);
                    lastWasDash = false;
                }
            } else {
                if (!lastWasDash) {
                    out.append('-');
                    lastWasDash = true;
                }
            }
        }

        out.append('-').append(hash);

        // Truncamento seguro sem alocações extras de substring
        if (out.length() > 80) {
            out.setLength(80);
        }
        return out.toString();
    }

    private String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        final String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private void applySecurity(final SdiaKafkaEndpoint endpoint, final Properties props) {
        final String authentication = endpoint.getAuthentication();
        final boolean tls = resolveTlsEnabled(endpoint, authentication);


        // mTLS — Mutual TLS: client certificate + private key from CPI Keystore.
        // No SASL layer. The broker authenticates the client via the presented certificate.
        if (endpoint.isMtlsAuthentication(authentication)) {
            props.put("security.protocol", "SSL");
            SdiaKafkaSslConfigurator.applySslMaterial(endpoint, props, true);
            return;
        }

        // NONE — no SASL/client certificate. In the simplified v1.5 UI,
        // Authentication=None is strictly PLAINTEXT. This prevents stale hidden
        // connectWithTls=true values from sending TLS ClientHello to PLAINTEXT listeners.
        if (isPlainTextAuthentication(authentication)) {
            props.put("security.protocol", "PLAINTEXT");
            return;
        }

        final boolean sasl = isSaslAuthentication(authentication);

        if (!sasl) {
            props.put("security.protocol", tls ? "SSL" : "PLAINTEXT");
            SdiaKafkaSslConfigurator.applySslMaterial(endpoint, props, tls);
            return;
        }

        final String mechanism = normalizeMechanism(endpoint.getSaslMechanism());
        props.put("security.protocol", tls ? "SASL_SSL" : "SASL_PLAINTEXT");
        props.put("sasl.mechanism", mechanism);

        final SdiaKafkaCredentials credentials = SdiaKafkaCredentialsResolver.resolve(endpoint.getCredentialAlias());
        props.put("sasl.jaas.config", buildJaasConfig(mechanism, credentials));
        SdiaKafkaSslConfigurator.applySslMaterial(endpoint, props, tls);
    }

    private boolean isPlainTextAuthentication(final String authentication) {
        final String normalized = normalizeAuthenticationToken(authentication);
        if (normalized == null) {
            return false;
        }

        // CPI can persist the fixed value ("None") or UI label variants.
        // This means "no SASL/user credential". It does NOT force PLAINTEXT;
        // TLS is decided by connectWithTls only for SASL. NONE is PLAINTEXT; mTLS always forces TLS.
        return "none".equals(normalized)
                || "plaintext".equals(normalized)
                || "plain text".equals(normalized)
                || "none plaintext".equals(normalized)
                || "none plain text".equals(normalized)
                || (normalized.indexOf("none") >= 0 && normalized.indexOf("plain") >= 0);
    }

    private boolean isSaslAuthentication(final String authentication) {
        final String normalized = normalizeAuthenticationToken(authentication);
        return normalized == null || normalized.indexOf("sasl") >= 0;
    }

    private boolean resolveTlsEnabled(final SdiaKafkaEndpoint endpoint, final String authentication) {
        final String normalized = normalizeAuthenticationToken(authentication);

        // mTLS is TLS-only. Ignore stale hidden
        // connectWithTls=false values persisted by older adapter metadata.
        if (endpoint != null && (endpoint.isMtlsAuthentication(authentication))) {
            return true;
        }

        // Explicit profile support for metadata values such as SASL_PLAINTEXT,
        // "NONE | Plain Text", "no TLS" or "without TLS".
        if (normalized != null
                && (normalized.indexOf("plaintext") >= 0
                    || normalized.indexOf("plain text") >= 0
                    || normalized.indexOf("no tls") >= 0
                    || normalized.indexOf("without tls") >= 0)) {
            return false;
        }

        return endpoint != null && Boolean.TRUE.equals(endpoint.getConnectWithTls());
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

    private String normalizeMechanism(final String mechanism) {
        if (mechanism == null || mechanism.trim().isEmpty()) {
            return "PLAIN";
        }
        // Evita chamadas pesadas de Locale convertendo via manipulação de caracteres padrão ASCII
        final String trimmed = mechanism.trim();
        final int len = trimmed.length();
        final StringBuilder sb = new StringBuilder(len);
        for (int i = 0; i < len; i++) {
            char c = trimmed.charAt(i);
            if (c >= 'a' && c <= 'z') {
                sb.append((char) (c - 32));
            } else {
                sb.append(c);
            }
        }
        final String normalized = sb.toString();
        if ("PLAIN".equals(normalized) || "SCRAM-SHA-256".equals(normalized) || "SCRAM-SHA-512".equals(normalized)) {
            return normalized;
        }
        throw new IllegalArgumentException("Unsupported SASL mechanism: " + mechanism);
    }

    private String buildJaasConfig(final String mechanism, final SdiaKafkaCredentials credentials) {
        final boolean isPlain = "PLAIN".equalsIgnoreCase(mechanism);
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
     * Otimização: Escreve os caracteres de escape diretamente no StringBuilder principal,
     * impedindo a instanciação de strings intermediárias geradas por métodos .replace().
     */
    private static void escapeToBuffer(final StringBuilder buffer, final String value) {
        if (value == null) return;
        final int len = value.length();
        for (int i = 0; i < len; i++) {
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
}