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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Scheduled-poll consumer for the EventSmartKafka Adapter.
 *
 * <h3>Critical PLAINTEXT fix (v1.0.1)</h3>
 * When {@code Authentication = NONE | Plain Text}, the consumer now sets
 * {@code security.protocol=PLAINTEXT} and exits the security configuration path
 * immediately — no JAAS context, no credential lookup, no SSL properties.
 * This eliminates the "Timeout expired while fetching topic metadata" failure
 * observed on On-Premise brokers such as {@code sdia-kafka:19092}.
 *
 * <h3>Port parameter (v1.0.1)</h3>
 * The {@code kafkaPortRow} channel parameter is now declared as
 * {@code xsd:string} in {@code metadata.xml} and handled as {@code String} in
 * {@link SdiaKafkaEndpoint#resolveBootstrapServers()}. This allows CPI external
 * parameters to bind to the port field. The previous {@code xsd:integer} type
 * prevented external parameter binding from the CPI UI.
 *
 * <h3>Schema Registry URL default (v1.0.1)</h3>
 * The {@code schemaRegistryHostAddress} field placeholder in metadata now shows
 * {@code https://} as the default hint, making it explicit that HTTPS is expected
 * for all Schema Registry connections.
 *
 * <h3>Authentication scope (v1.0)</h3>
 * OAuth 2.0, mTLS, and Client Certificate auth profiles are removed from this
 * release. Supported profiles: NONE/PLAINTEXT, SASL/PLAIN, SASL/SCRAM-SHA-256,
 * SASL/SCRAM-SHA-512, TLS (SSL, server-side only).
 *
 * <h3>Hotpath design</h3>
 * <ul>
 *   <li>Final fields for frequently accessed adapter parameters cached at
 *       initialisation — avoids repeated virtual dispatch through the endpoint
 *       on every {@code poll()} iteration.</li>
 *   <li>All {@code log.debug} and {@code log.trace} calls are guarded with
 *       {@code if (LOG.isDebugEnabled())} to prevent String concatenation when
 *       the debug level is inactive.</li>
 *   <li>Pre-allocated hex char array for offset header construction.</li>
 *   <li>{@link LinkedHashMap} used for offset maps to preserve insertion order
 *       and keep commit logging deterministic.</li>
 * </ul>
 *
 * <h3>Unchanged logic</h3>
 * Poison pill skip, seek, commit, silent commit, EARLIEST/LATEST/NONE offset
 * reset, retry, Stop-on-Error, Message Recovery single-record mode, and the
 * Cloud Connector SOCKS5 tunnel are carried forward without modification.
 */
public class SdiaKafkaConsumer extends ScheduledPollConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SdiaKafkaConsumer.class);

    // ── Pre-allocated constants ──────────────────────────────────────────────

    /** Hex digits for the payload preview header — allocated once, shared. */
    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private static final long INIT_FAILURE_RETRY_BACKOFF_MS   = 5_000L;
    private static final long AVRO_FAILURE_RETRY_BACKOFF_MS   = 5_000L;
    private static final long EMPTY_POLL_DIAG_LOG_EVERY_MS    = 30_000L;

    // ── Adapter endpoint ─────────────────────────────────────────────────────

    /** Immutable reference to the endpoint — safe for hotpath access. */
    private final SdiaKafkaEndpoint endpoint;

    // ── Lifecycle state ──────────────────────────────────────────────────────

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private boolean assignMode = false;
    private String  effectiveGroupId = "unknown";
    private boolean initialized = false;

    // ── Message Recovery state ───────────────────────────────────────────────

    private volatile boolean recoverySeekApplied = false;
    private volatile long    recoverySeekTimestampMs = -1L;
    private volatile String  recoverySeekInput = null;

    private final Map<TopicPartition, Long>              recoverySeekOffsets              = new LinkedHashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> recoveryOriginalCommittedOffsets = new LinkedHashMap<>();

    /**
     * Terminal guard for "Read only first matched message" recovery mode.
     * Set to {@code true} after the single recovered record is processed.
     * poll() returns 0 until recovery is disabled and the iFlow is redeployed.
     */
    private volatile boolean recoverySingleMessageCompleted   = false;
    private volatile long    recoveryCompletedOffset          = -1L;
    private volatile String  recoveryCompletedTopicPartition  = null;

    // ── Error-handling state ─────────────────────────────────────────────────

    /**
     * Avro/schema conversion failures are adapter-level failures.
     * The offset is NOT committed. The consumer seeks back and retries
     * after {@link #AVRO_FAILURE_RETRY_BACKOFF_MS}.
     */
    private volatile boolean fatalAvroConversionFailure = false;
    private volatile String  fatalAvroConversionReason  = null;
    private volatile long    nextAvroRetryAllowedAtMs   = 0L;

    /**
     * "Stop on Error" is terminal for the deployed consumer instance.
     * The failed offset is NOT committed. The KafkaConsumer is closed
     * to release the partition assignment immediately.
     */
    private volatile boolean stoppedByErrorPolicy       = false;
    private volatile String  stoppedByErrorPolicyReason = null;

    /**
     * Initialisation failures with retryable network causes are retried after
     * {@link #INIT_FAILURE_RETRY_BACKOFF_MS}. Non-retryable configuration
     * failures set {@link #nonRetryableConfigurationFailure} and remain silent
     * until redeploy.
     */
    private volatile boolean fatalInitFailure                 = false;
    private volatile String  fatalInitReason                  = null;
    private volatile long    nextInitRetryAllowedAtMs         = 0L;
    private volatile boolean nonRetryableConfigurationFailure = false;

    /** Rate-limiter for empty-poll heartbeat log lines. */
    private volatile long lastEmptyPollLogAtMs = 0L;

    /**
     * Reusable hex preview builder for the {@code x-sdiakafka-payload-first-bytes-hex} header.
     * Max content: {@code getEffectiveSchemaIdSizeBuffer() * 3 = 48 chars}. Pre-allocated once.
     * Safe for reuse: single Camel scheduler thread, cleared before each record.
     */
    private final StringBuilder payloadHexPreview = new StringBuilder(64);

    /**
     * Reusable offset accumulator for the poll() batch.
     * Pre-allocated once per consumer lifetime — cleared at the start of each
     * non-empty poll batch instead of being allocated anew. Safe because the
     * Kafka consumer is always accessed from a single Camel scheduler thread.
     * Initial capacity 4: most channels have 1–4 partitions.
     */
    private final Map<TopicPartition, OffsetAndMetadata> pollSuccessOffsets = new LinkedHashMap<>(4);

    // ── On-Premise Cloud Connector TCP tunnel ─────────────────────────────────

    /**
     * SAP Cloud Connector TCP bridge for On-Premise Kafka brokers.
     *
     * Kafka's Java client does not natively traverse SAP Cloud Connector virtual
     * hosts. In On-Premise mode the consumer starts a local SOCKS5 tunnel and
     * rewrites {@code bootstrap.servers} to {@code 127.0.0.1:<port>}. The tunnel
     * forwards the raw Kafka TCP stream through the SAP Connectivity SOCKS5 proxy.
     *
     * Broker requirement: Kafka {@code advertised.listeners} for the external
     * listener must advertise {@code localhost:<port>} or {@code 127.0.0.1:<port>}
     * so that broker metadata directs reconnects back through the local tunnel port.
     */
    private volatile SdiaCloudConnectorTcpTunnel onPremiseTcpTunnel;
    private volatile String                      onPremiseClientBootstrapServers;
    private volatile boolean                     onPremiseForcedAssignMode = false;

    // =========================================================================
    // Constructor
    // =========================================================================

    public SdiaKafkaConsumer(final SdiaKafkaEndpoint endpoint, final Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;

        // ADK/CPI contract: poll immediately on deploy. Hidden channel scheduling
        // values must not keep the route idle for minutes after a redeploy.
        this.setInitialDelay(0L);
        this.setDelay(1_000L);
        this.setUseFixedDelay(true);
        try { this.setGreedy(true); } catch (Throwable ignored) {}
    }

    // =========================================================================
    // Lifecycle — doStart / doStop
    // =========================================================================

    @Override
    protected void doStart() throws Exception {
        // Reset all volatile state atomically before handing control to the scheduler.
        closed.set(false);
        kafkaConsumer                    = null;
        initialized                      = false;
        fatalAvroConversionFailure       = false;
        fatalAvroConversionReason        = null;
        recoverySingleMessageCompleted   = false;
        recoveryCompletedOffset          = -1L;
        recoveryCompletedTopicPartition  = null;
        recoverySeekApplied              = false;
        recoverySeekTimestampMs          = -1L;
        recoverySeekInput                = null;
        recoverySeekOffsets.clear();
        recoveryOriginalCommittedOffsets.clear();
        nextAvroRetryAllowedAtMs         = 0L;
        stoppedByErrorPolicy             = false;
        stoppedByErrorPolicyReason       = null;
        fatalInitFailure                 = false;
        fatalInitReason                  = null;
        nextInitRetryAllowedAtMs         = 0L;
        nonRetryableConfigurationFailure = false;
        lastEmptyPollLogAtMs             = 0L;
        onPremiseClientBootstrapServers  = null;
        onPremiseForcedAssignMode        = false;
        closeOnPremiseTcpTunnelSilently();

        // Purge deployment-scoped caches so rotated credentials and re-registered
        // schemas take effect without requiring a platform restart.
        try { SdiaKafkaCredentialsResolver.clearCache(); } catch (Throwable ignored) {}
        try { SdiaSchemaRegistryClient.clearCache();     } catch (Throwable ignored) {}

        // Re-apply scheduling overrides — ScheduledPollConsumer may restore
        // serialised values from a previous run during the super.doStart() call.
        this.setInitialDelay(0L);
        this.setDelay(1_000L);
        this.setUseFixedDelay(true);
        try { this.setGreedy(true); } catch (Throwable ignored) {}

        // Install a custom exception handler that routes adapter errors back through
        // the Camel pipeline so they appear in the CPI Message Monitor.
        this.setExceptionHandler(new org.apache.camel.spi.ExceptionHandler() {
            @Override
            public void handleException(Throwable ex) {
                handleException("Kafka polling error", null, ex);
            }
            @Override
            public void handleException(String msg, Throwable ex) {
                handleException(msg, null, ex);
            }
            @Override
            public void handleException(String msg, Exchange exchange, Throwable ex) {
                LOG.error("[SDIA Kafka] Polling exception caught: {}", ex.getMessage());
                try {
                    final Exchange errorExchange = (exchange != null)
                            ? exchange
                            : endpoint.createExchange();
                    errorExchange.setException(
                            (ex instanceof RuntimeCamelException)
                                    ? ex
                                    : new RuntimeCamelException(ex.getMessage(), ex));
                    getProcessor().process(errorExchange);
                } catch (Throwable t) {
                    LOG.error("[SDIA Kafka] ExceptionHandler delivery failed: {}", t.getMessage());
                }
            }
        });

        LOG.info("[SDIA Kafka] doStart() complete — Kafka consumer will initialise on first poll.");

        try {
            final String topic     = endpoint.getTopicPattern() != null ? endpoint.getTopicPattern() : "unknown-topic";
            final String bootstrap = endpoint.resolveBootstrapServers() != null ? endpoint.resolveBootstrapServers() : "unknown-host";
            final String iFlowId   = endpoint.getCamelContext() != null ? endpoint.getCamelContext().getName() : topic;
            SdiaKafkaEndpointInformationService.register(iFlowId, topic, bootstrap, "Starting...");
        } catch (Throwable ignored) {}

        super.doStart();
    }

    @Override
    protected void doStop() throws Exception {
        closed.set(true);
        closeKafkaConsumerSilently();
        closeOnPremiseTcpTunnelSilently();
        super.doStop();
    }

    // =========================================================================
    // poll() — main hotpath
    // =========================================================================

    @Override
    protected int poll() throws Exception {
        if (closed.get()) {
            return 0;
        }

        // ── Terminal guard: Stop on Error ──────────────────────────────────
        if (stoppedByErrorPolicy) {
            // Do NOT heartbeat-poll: a paused live consumer can hold the partition
            // assignment and cause a 5-10 minute "zombie" after redeploy.
            return 0;
        }

        // ── Terminal guard: single-message recovery completed ──────────────
        if (recoverySingleMessageCompleted) {
            return 0;
        }

        // ── Avro failure backoff ────────────────────────────────────────────
        if (fatalAvroConversionFailure) {
            if (System.currentTimeMillis() < nextAvroRetryAllowedAtMs) {
                return 0;
            }
            fatalAvroConversionFailure = false;
        }

        // ── Init failure backoff / non-retryable gate ───────────────────────
        if (fatalInitFailure) {
            if (nonRetryableConfigurationFailure) {
                return 0;
            }
            if (System.currentTimeMillis() < nextInitRetryAllowedAtMs) {
                return 0;
            }
            LOG.warn("[SDIA Kafka] Init failure backoff complete. Retrying. Last reason: {}",
                    fatalInitReason != null ? fatalInitReason : "<empty>");
            fatalInitFailure = false;
            fatalInitReason  = null;
            initialized      = false;
            closeKafkaConsumerSilently();
        }

        // ── Lazy initialisation ─────────────────────────────────────────────
        if (!initialized) {
            final String initError = tryDelayedInitialize();
            if (initError != null) {
                fatalInitFailure             = true;
                fatalInitReason              = initError;
                nextInitRetryAllowedAtMs     = System.currentTimeMillis() + INIT_FAILURE_RETRY_BACKOFF_MS;
                surfaceErrorToChannel(initError);
                return 0;
            }
        }

        if (kafkaConsumer == null) {
            final String err = "KafkaConsumer null after initialisation — state will be recycled and retried.";
            fatalInitFailure         = true;
            fatalInitReason          = err;
            nextInitRetryAllowedAtMs = System.currentTimeMillis() + INIT_FAILURE_RETRY_BACKOFF_MS;
            initialized              = false;
            surfaceErrorToChannel(err);
            return 0;
        }

        // ── Kafka poll ──────────────────────────────────────────────────────
        ConsumerRecords<String, byte[]> records;
        try {
            long timeoutMs = endpoint.getPollTimeoutMs();
            if (timeoutMs <= 0L) {
                timeoutMs = 2_000L;
            }
            // On-Premise tunnels are sensitive to long poll timeouts: cap at 2 s to
            // prevent the Cloud Connector SOCKS5 idle-close from looking like a hang.
            if (isOnPremiseProxyMode() && timeoutMs > 2_000L) {
                timeoutMs = 2_000L;
            }
            records = kafkaConsumer.poll(Duration.ofMillis(timeoutMs));
        } catch (WakeupException we) {
            if (closed.get()) {
                return 0;
            }
            throw we;
        } catch (Throwable pollEx) {
            final String err;
            if (isNoOffsetForPartitionFailure(pollEx)) {
                err = buildVisualError(
                        "Kafka Auto Offset Reset NONE has no committed offset.",
                        "Consumer Group: " + effectiveGroupId
                                + "\nTopic: " + endpoint.getTopicPattern()
                                + "\nAuto Offset Reset: " + endpoint.getAutoOffsetReset()
                                + "\nDetail: " + rootCauseMessage(pollEx),
                        "1 - Use EARLIEST to start from the oldest retained offset."
                                + "\n2 - Use LATEST to wait for new messages only."
                                + "\n3 - Keep NONE only when a committed offset already exists for this consumer group.");
                nonRetryableConfigurationFailure = true;
            } else {
                err = "Kafka poll() failed. Consumer will be recycled. Detail: " + rootCauseMessage(pollEx);
            }
            LOG.error("[SDIA Kafka] {}", err, pollEx);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), err); } catch (Throwable ignored) {}
            closeKafkaConsumerSilently();
            initialized              = false;
            fatalInitFailure         = true;
            fatalInitReason          = err;
            nextInitRetryAllowedAtMs = System.currentTimeMillis() + INIT_FAILURE_RETRY_BACKOFF_MS;
            try { surfaceErrorToChannel(err); } catch (Throwable ignored) {}
            return 0;
        }

        if (records == null || records.isEmpty()) {
            logEmptyPollHeartbeat();
            return 0;
        }

        LOG.warn("[SDIA Kafka] POLL BATCH group.id=" + effectiveGroupId
                + " count=" + records.count()
                + " partitions=" + records.partitions());

        int delivered = 0;
        // Reuse the pre-allocated offset map — clear is O(n) where n = partitions (usually 1-4).
        pollSuccessOffsets.clear();
        final Map<TopicPartition, OffsetAndMetadata> successOffsets = pollSuccessOffsets;

        for (final ConsumerRecord<String, byte[]> record : records) {
            if (closed.get() || fatalAvroConversionFailure) {
                break;
            }

            Exchange exchange = null;
            try {
                exchange = processRecord(record);

                if (exchange.getException() != null) {
                    final Throwable conversionEx   = exchange.getException();
                    final String originalMessage   = conversionEx.getMessage() != null ? conversionEx.getMessage() : "Unknown exception root cause";
                    final String errCode           = String.valueOf(exchange.getIn().getHeader("x-sdiakafka-error-code",      String.class));
                    final String schemaMode        = String.valueOf(exchange.getIn().getHeader("x-sdiakafka-schema-mode",      String.class));
                    final Object schemaId          = exchange.getIn().getHeader("x-sdiakafka-resolved-schema-id");

                    final String handling          = normalizeErrorHandling(endpoint.getErrorHandling());
                    final boolean skipPoison       = "Skip Failed Message".equalsIgnoreCase(handling);
                    final String fatalMsg          = buildCompactAvroError(record, errCode, schemaMode, schemaId, originalMessage, skipPoison);

                    if (skipPoison) {
                        commitAvroPoisonOffsetThenRecycle(record, fatalMsg, conversionEx);
                        if (shouldStopAfterRecoveredRecord(record)) {
                            markRecoverySingleMessageCompleted(record, "skipped after conversion failure");
                            return delivered + 1;
                        }
                        throw new RuntimeCamelException(fatalMsg, conversionEx);
                    }

                    commitSuccessOffsets(successOffsets, delivered);

                    if ("Stop on Error".equalsIgnoreCase(handling)) {
                        publishStopErrorExchangeToIFlow(exchange, record, fatalMsg, conversionEx, "Avro conversion failure");
                        haltConsumerAfterFatalAvroFailure(record, fatalMsg, conversionEx);
                        return delivered;
                    }

                    haltConsumerAfterFatalAvroFailure(record, fatalMsg, conversionEx);
                    throw new RuntimeCamelException(fatalMsg, conversionEx);
                }

                processPipelineExchangeWithPolicy(exchange);

                final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                successOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
                delivered++;

                if (shouldStopAfterRecoveredRecord(record)) {
                    restoreOriginalCommittedOffsetsAfterSingleRecovery(record, "success");
                    markRecoverySingleMessageCompleted(record, "success");
                    return delivered;
                }

            } catch (Throwable ex) {
                if (ex instanceof RuntimeCamelException
                        && ex.getMessage() != null
                        && ex.getMessage().contains("Avro Conversion Failure")) {
                    throw (RuntimeCamelException) ex;
                }

                final String errorDetail     = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getName();
                final String processorFatal  = buildVisualError(
                        "Pipeline Processor Break.",
                        "Record Topic: "    + record.topic()
                                + "\nTarget Offset: "    + record.offset()
                                + "\nTarget Partition: " + record.partition()
                                + "\nDetail: "           + errorDetail,
                        "Verify your Integration Flow processing steps downstream.");

                if (exchange != null) {
                    exchange.setException(new RuntimeCamelException(processorFatal));
                }

                LOG.error("[SDIA Kafka] {}", processorFatal, ex);

                final String handling = normalizeErrorHandling(endpoint.getErrorHandling());

                if ("Skip Failed Message".equalsIgnoreCase(handling)) {
                    final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    successOffsets.put(tp, new OffsetAndMetadata(record.offset() + 1));
                    delivered++;
                    if (shouldStopAfterRecoveredRecord(record)) {
                        restoreOriginalCommittedOffsetsAfterSingleRecovery(record, "skipped after pipeline failure");
                        markRecoverySingleMessageCompleted(record, "skipped after pipeline failure");
                        return delivered;
                    }
                    continue;
                }

                commitSuccessOffsets(successOffsets, delivered);
                haltConsumerAfterPipelineFailure(record, processorFatal, ex);

                if ("Stop on Error".equalsIgnoreCase(handling)) {
                    return delivered;
                }

                if (ex instanceof Exception) {
                    throw (Exception) ex;
                }
                throw new RuntimeCamelException(ex);
            }
        }

        commitSuccessOffsets(successOffsets, delivered);
        return delivered;
    }

    // =========================================================================
    // Delayed initialisation
    // =========================================================================

    private synchronized String tryDelayedInitialize() {
        if (initialized) {
            return null;
        }
        LOG.warn("[SDIA Kafka] ===== tryDelayedInitialize() START =====");
        final String result = doInitialize();
        if (result != null) {
            LOG.error("[SDIA Kafka] tryDelayedInitialize() FAILED: {}", result);
        } else {
            LOG.warn("[SDIA Kafka] tryDelayedInitialize() SUCCESS");
        }
        return result;
    }

    /**
     * Performs the full consumer initialisation sequence:
     * configuration validation → credential verification (SASL only) →
     * optional Cloud Connector tunnel → Kafka admin topic probe →
     * consumer construction → subscribe/assign → optional Message Recovery seek.
     *
     * Returns {@code null} on success, or a human-readable error string on failure.
     * The error string is surfaced to the CPI Message Monitor via
     * {@link #surfaceErrorToChannel(String)}.
     *
     * <h3>Authentication guard (PLAINTEXT)</h3>
     * When authentication is {@code NONE | PLAINTEXT}, the credential alias is
     * logged as ignored and no lookup is performed — preventing the
     * "SecureStoreService alias not found" cascade that previously triggered
     * a timeout on plain-text brokers.
     */
    private String doInitialize() {
        // ── Step 1: configuration validation ─────────────────────────────
        try {
            validateConfiguration();
            endpoint.validateConfiguration();
        } catch (IllegalArgumentException cfgEx) {
            nonRetryableConfigurationFailure = true;
            final String errorMsg = buildAdapterConfigurationError(cfgEx);
            LOG.error("[SDIA Kafka] {}", errorMsg, cfgEx);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
            closeKafkaConsumerSilently();
            return errorMsg;
        }

        // ── Step 2: conversion startup log ───────────────────────────────
        final String startupConversionFormat = normalizeConversionFormat(endpoint.getEffectiveConversionFormat());
        if (!"None".equalsIgnoreCase(startupConversionFormat)) {
            LOG.warn("[SDIA Kafka] [CONVERSION] Conversion=" + startupConversionFormat
                    + " | Mode=" + trimToNull(endpoint.getSchemaIdResolutionMode())
                    + " | Registry=" + maskUrl(trimToNull(endpoint.getSchemaRegistryHostAddress()))
                    + " | CredentialAlias=" + (trimToNull(endpoint.getSchemaRegistryCredentialAlias()) == null ? "<empty>" : endpoint.getSchemaRegistryCredentialAlias())
                    + " | FixedSchemaId=" + (trimToNull(endpoint.getSchemaRegistrySchemaId()) == null ? "<empty>" : endpoint.getSchemaRegistrySchemaId())
                    + " | Runtime lookup on first record.");
        }

        // ── Step 3: credential validation (SASL only, never for PLAINTEXT) ──
        final String brokerAlias = trimToNull(endpoint.getCredentialAlias());

        if (isPlainTextAuthentication(endpoint.getAuthentication())) {
            // ---------------------------------------------------------------
            // PLAINTEXT path: zero credential logic.
            // Log a clear message so operators understand why no alias is used,
            // and why any configured alias is intentionally ignored.
            // ---------------------------------------------------------------
            if (brokerAlias != null) {
                LOG.warn("[SDIA Kafka] Authentication is NONE | PLAINTEXT. "
                        + "Credential Alias [{}] is configured but will NOT be used — "
                        + "no SAP CPI Secure Store lookup is performed for PLAINTEXT connections.", brokerAlias);
            } else {
                LOG.info("[SDIA Kafka] Authentication is NONE | PLAINTEXT. "
                        + "No Credential Alias configured. No credential lookup performed.");
            }
        } else if (isSaslAuthentication(endpoint.getAuthentication())) {
            if (brokerAlias == null) {
                nonRetryableConfigurationFailure = true;
                final String errorMsg = buildVisualError(
                        "Kafka Broker Credential verification fail.",
                        "Authentication: " + endpoint.getAuthentication()
                                + "\nTarget Alias: <empty>"
                                + "\nCluster Host: " + endpoint.resolveBootstrapServers(),
                        "1 - SASL requires a CPI Security Material entry of type User Credentials."
                                + "\n2 - If the broker is unauthenticated PLAINTEXT, select Authentication = NONE | Plain Text.");
                LOG.error("[SDIA Kafka] {}", errorMsg);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                return errorMsg;
            }
            try {
                SdiaKafkaCredentialsResolver.resolve(brokerAlias);
            } catch (Throwable credentialEx) {
                nonRetryableConfigurationFailure = true;
                final String cause    = rootCauseMessage(credentialEx);
                final String errorMsg = buildVisualError(
                        "Kafka Broker Credential verification fail.",
                        "Target Alias: " + brokerAlias
                                + "\nCluster Host: " + endpoint.resolveBootstrapServers()
                                + "\nError detail: " + cause,
                        "1 - Deploy a User Credentials or Secure Parameter in Security Materials named exactly: " + brokerAlias
                                + "\n2 - Verify reference settings.");
                LOG.error("[SDIA Kafka] {}", errorMsg, credentialEx);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                return errorMsg;
            }
        }

        // ── Step 4: bootstrap server resolution ───────────────────────────
        final String bootstrapServers = endpoint.resolveBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            nonRetryableConfigurationFailure = true;
            final String errorMsg = buildVisualError(
                    "Kafka Bootstrap Servers not configured.",
                    "kafkaClusterHostsTable is empty and no fallback host was provided.",
                    "1 - Fill in at least one row in the Kafka Cluster Bootstrap Servers table."
                            + "\n2 - Add the broker host and port (default 9092).");
            LOG.error("[SDIA Kafka] {}", errorMsg);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
            return errorMsg;
        }

        // ── Step 5: Cloud Connector tunnel (On-Premise only) ──────────────
        String kafkaClientBootstrapServers = bootstrapServers;
        if (isOnPremiseProxyMode()) {
            try {
                kafkaClientBootstrapServers = ensureOnPremiseTcpTunnel(bootstrapServers);
                LOG.warn("[SDIA Kafka] On-Premise Cloud Connector tunnel active. virtualBootstrap=" + bootstrapServers
                        + " localKafkaBootstrap=" + kafkaClientBootstrapServers
                        + " locationId=" + displayOptionalLocationId(endpoint.getSapSapccLocationId()));
            } catch (Throwable tunnelEx) {
                nonRetryableConfigurationFailure = true;
                final String errorMsg = buildVisualError(
                        "Adapter Configuration Failure - Cloud Connector TCP Tunnel Failed",
                        "➔ Proxy Type: " + endpoint.getProxyType()
                                + "\n➔ Virtual Bootstrap Servers: " + bootstrapServers
                                + "\n➔ Topic: " + endpoint.getTopicPattern()
                                + "\n➔ Location ID: " + displayOptionalLocationId(endpoint.getSapSapccLocationId())
                                + "\n🔴 Error Code: Event.Smart.Kafka.Config.CloudConnector.Tunnel.Failed"
                                + "\n🔴 Error detail: " + rootCauseMessage(tunnelEx),
                        "1 - In Cloud Connector, map the virtual Kafka host/port to the internal broker host/port."
                                + "\n2 - Location ID is optional. Leave it empty when the Cloud Connector subaccount connection has no Location ID."
                                + "\n3 - For Kafka/TCP the consumer uses the SAP Connectivity SOCKS5 proxy, not HTTP CONNECT."
                                + "\n4 - Keep Kafka advertised.listeners for the client listener as localhost:<port> or 127.0.0.1:<port>."
                                + "\n5 - Check that the local tunnel port is not already occupied by another adapter instance."
                                + "\n6 - Redeploy after fixing the Cloud Connector mapping or Kafka advertised listener.");
                LOG.error("[SDIA Kafka] {}", errorMsg, tunnelEx);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeOnPremiseTcpTunnelSilently();
                return errorMsg;
            }
        }

        LOG.warn("[SDIA Kafka] Bootstrap resolved. bootstrapServers={} kafkaClientBootstrap={}",
                bootstrapServers, kafkaClientBootstrapServers);

        // ── Step 6: consumer construction + admin topic probe ─────────────
        // KafkaConsumer and the admin probe share a ClassLoader context so that
        // the JAAS login modules (PlainLoginModule, ScramLoginModule) are visible
        // even inside OSGi environments where the system classloader may not
        // have loaded the kafka-clients bundle.
        final ClassLoader originalCl     = Thread.currentThread().getContextClassLoader();
        final ClassLoader kafkaCl        = resolveKafkaClassLoader();
        final KafkaConsumerFactory factory = new KafkaConsumerFactory();

        try {
            Thread.currentThread().setContextClassLoader(kafkaCl);

            final Properties props = factory.createProperties(endpoint);
            applyOnPremiseClientBootstrapIfRequired(props, kafkaClientBootstrapServers);
            applyConsumerRuntimeProperties(props, false);

            LOG.warn("[SDIA Kafka] Kafka security profile resolved."
                    + " authentication=" + endpoint.getAuthentication()
                    + " tls=" + endpoint.isTlsEnabledEffective()
                    + " sasl=" + endpoint.isAuthenticationSaslEffective()
                    + " securityProtocol=" + props.get("security.protocol")
                    + " saslMechanism=" + (props.get("sasl.mechanism") != null ? props.get("sasl.mechanism") : "n/a")
                    + " trustSource=" + (endpoint.isTlsEnabledEffective() ? endpoint.getTlsTrustSourceEffective() : "n/a")
                    + " credentialAlias=" + (endpoint.isAuthenticationSaslEffective()
                            ? (trimToNull(endpoint.getCredentialAlias()) != null ? "configured" : "<empty>")
                            : "n/a - NONE does not use credentials")
                    + " onPremise=" + isOnPremiseProxyMode());

            // Admin probe: list topics with a short, bounded timeout so a
            // misconfigured broker fails fast rather than blocking the hotpath
            // for the full default Kafka metadata fetch timeout (60 s).
            final Map<String, List<PartitionInfo>> knownTopics;
            try {
                final Properties adminProps = factory.createProperties(endpoint);
                applyOnPremiseClientBootstrapIfRequired(adminProps, kafkaClientBootstrapServers);
                adminProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
                applyConsumerRuntimeProperties(adminProps, true /* adminMode */);

                final KafkaConsumer<String, byte[]> adminConsumer = new KafkaConsumer<>(adminProps);
                try {
                    knownTopics = adminConsumer.listTopics(Duration.ofMillis(4_000));
                } finally {
                    try { adminConsumer.close(Duration.ZERO); } catch (Throwable ignored) {}
                }
            } catch (Throwable netEx) {
                nonRetryableConfigurationFailure = true;
                final String errorMsg = buildKafkaBrokerConnectionConfigurationError(
                        rootCauseMessage(netEx), endpoint.resolveBootstrapServers());
                LOG.error("[SDIA Kafka] {}", errorMsg, netEx);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return errorMsg;
            }

            this.kafkaConsumer = new KafkaConsumer<>(props);

            // ── Step 7: subscribe / assign ─────────────────────────────────
            final String partitionsParam  = trimToNull(endpoint.getPartitions());
            final String topicExpression  = trimToNull(endpoint.getTopicPattern());
            final List<String> topicEntries = parseTopicExpression(topicExpression);
            this.effectiveGroupId         = String.valueOf(props.get(ConsumerConfig.GROUP_ID_CONFIG));

            if (this.effectiveGroupId == null
                    || this.effectiveGroupId.trim().isEmpty()
                    || "null".equalsIgnoreCase(this.effectiveGroupId.trim())) {
                nonRetryableConfigurationFailure = true;
                final String errorMsg = buildVisualError(
                        "Kafka Consumer Group ID not configured.",
                        "group.id resolved to empty/null. Without a stable group.id, Kafka cannot persist committed offsets.",
                        "Set a stable Group ID in the adapter channel. Do not generate a random group.id per deployment.");
                LOG.error("[SDIA Kafka] {}", errorMsg);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return errorMsg;
            }

            LOG.warn("[SDIA Kafka] Consumer runtime initialised. group.id=" + this.effectiveGroupId
                    + " assignMode=" + (partitionsParam != null)
                    + " onPremise=" + isOnPremiseProxyMode()
                    + " topicExpression=" + topicExpression);

            this.onPremiseForcedAssignMode = false;

            if (partitionsParam != null) {
                if (topicEntries.size() != 1 || isPatternMode(topicEntries.get(0))) {
                    nonRetryableConfigurationFailure = true;
                    final String errorMsg = buildVisualError(
                            "Explicit Partitions subscription mismapped.",
                            "Configured Topics: " + topicExpression
                                    + "\nDetail: Explicit partitions can only be used with one single exact topic.",
                            "1 - Leave Partitions field empty for wildcards.\n2 - Review parameters mappings.");
                    LOG.error("[SDIA Kafka] {}", errorMsg);
                    try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                    closeKafkaConsumerSilently();
                    return errorMsg;
                }

                final List<TopicPartition> topicPartitions = parsePartitions(topicEntries.get(0), partitionsParam);
                assignMode = true;
                kafkaConsumer.assign(topicPartitions);
                applyAssignedStartOffsets(topicPartitions,
                        (isOnPremiseProxyMode() ? "on-premise" : "cloud") + " explicit channel partitions");
            } else {
                assignMode = false;
                if (containsPatternMode(topicEntries)) {
                    final Pattern pattern = compileTopicPatternExpression(topicEntries);
                    kafkaConsumer.subscribe(pattern);
                    LOG.warn("[SDIA Kafka] Consumer subscribed by topic pattern. Normal Kafka group management active."
                            + " onPremise=" + isOnPremiseProxyMode()
                            + " group.id=" + effectiveGroupId
                            + " topicPattern=" + topicExpression);
                } else {
                    final List<String> missingTopics = findMissingTopics(knownTopics, topicEntries);
                    if (!missingTopics.isEmpty()) {
                        nonRetryableConfigurationFailure = true;
                        final String errorMsg = buildVisualError(
                                "Target Cluster Topic footprint not found.",
                                "Missing Topic(s): " + missingTopics,
                                "1 - Create the target topic in your Kafka cluster.\n2 - Check spelling and naming casing.");
                        LOG.error("[SDIA Kafka] {}", errorMsg);
                        try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                        closeKafkaConsumerSilently();
                        return errorMsg;
                    }
                    kafkaConsumer.subscribe(topicEntries);
                    LOG.warn("[SDIA Kafka] Consumer subscribed by exact topic list. Normal Kafka group management active."
                            + " onPremise=" + isOnPremiseProxyMode()
                            + " group.id=" + effectiveGroupId
                            + " topics=" + topicEntries);
                }
            }

            // ── Step 8: Message Recovery seek ─────────────────────────────
            final String recoveryError = applyMessageRecoverySeekIfConfigured(topicEntries, knownTopics, partitionsParam);
            if (recoveryError != null) {
                nonRetryableConfigurationFailure = true;
                LOG.error("[SDIA Kafka] {}", recoveryError);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), recoveryError); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return recoveryError;
            }

            // ── Step 9: register success ───────────────────────────────────
            final String iFlowId   = endpoint.getCamelContext() != null ? endpoint.getCamelContext().getName() : topicExpression;
            SdiaKafkaEndpointInformationService.register(
                    iFlowId,
                    topicExpression != null ? topicExpression : "unknown",
                    endpoint.resolveBootstrapServers() != null ? endpoint.resolveBootstrapServers() : "unknown-host",
                    "Successful");

            this.initialized = true;
            return null;

        } catch (Exception e) {
            nonRetryableConfigurationFailure = true;
            final String errorMsg = buildVisualError(
                    "Startup unexpected critical cluster failure.",
                    "Error detail: " + rootCauseMessage(e),
                    "1 - Review bundle properties.\n2 - Verify network topology version compatibility.");
            LOG.error("[SDIA Kafka] {}", errorMsg, e);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
            closeKafkaConsumerSilently();
            return errorMsg;
        } finally {
            Thread.currentThread().setContextClassLoader(originalCl);
        }
    }

    // =========================================================================
    // Consumer runtime properties
    // =========================================================================

    /**
     * Applies channel timeout/fetch/session parameters to {@code props}.
     *
     * <p>In admin-mode (topic-probe call), all blocking timeouts are capped at
     * 10 s to prevent the admin probe from occupying the Camel worker thread for
     * the full user-configured timeout during broker connectivity failures.
     *
     * <p>On-Premise mode clamps {@code fetch.max.wait.ms} to 1 s and sets
     * {@code client.dns.lookup=use_all_dns_ips} + {@code connections.max.idle.ms=30000}
     * to improve resilience through the Cloud Connector tunnel.
     *
     * @param props     properties to mutate
     * @param adminMode when {@code true}, cap blocking timeouts at 10 s
     */
    private void applyConsumerRuntimeProperties(final Properties props,
                                                final boolean adminMode) {
        final int requestTimeoutMs     = safeSecondsToMillis(endpoint.getRequestTimeoutS(),    30);
        final int sessionTimeoutMs     = safeSecondsToMillis(endpoint.getSessionTimeoutS(),    10);
        final int heartbeatIntervalMs  = safeSecondsToMillis(endpoint.getHeartbeatIntervalS(),  3);
        final int reconnectBackoffMs   = safeSecondsToMillis(endpoint.getReconnectDelayS(),     1);
        final int effectiveRequestMs   = adminMode ? Math.min(requestTimeoutMs, 10_000) : requestTimeoutMs;

        props.put("request.timeout.ms",       String.valueOf(effectiveRequestMs));
        props.put("default.api.timeout.ms",   String.valueOf(effectiveRequestMs));
        props.put("max.block.ms",             String.valueOf(effectiveRequestMs));
        props.put("metadata.max.age.ms",      "10000");

        props.put("fetch.min.bytes",          String.valueOf(endpoint.getMinFetchSizeBytes()));
        props.put("fetch.max.bytes",          String.valueOf(endpoint.getMaxFetchSizeBytes()));
        props.put("max.partition.fetch.bytes",String.valueOf(endpoint.getMaxPartitionFetchSizeBytes()));

        int fetchMaxWaitMs = endpoint.getMaxFetchWaitTime();
        if (isOnPremiseProxyMode() && fetchMaxWaitMs > 1_000) {
            fetchMaxWaitMs = 1_000;
        }
        props.put("fetch.max.wait.ms", String.valueOf(fetchMaxWaitMs));

        if (isOnPremiseProxyMode()) {
            props.put("client.dns.lookup",      "use_all_dns_ips");
            props.put("connections.max.idle.ms","30000");
        }

        final int configuredMaxPollRecords = endpoint.getMaxPollRecords() != null
                ? endpoint.getMaxPollRecords().intValue() : 1;
        props.put("max.poll.records",        String.valueOf(configuredMaxPollRecords));
        props.put("session.timeout.ms",      String.valueOf(sessionTimeoutMs));
        props.put("heartbeat.interval.ms",   String.valueOf(heartbeatIntervalMs));
        props.put("max.poll.interval.ms",    String.valueOf(endpoint.getMaxPollIntervalMs()));
        props.put("reconnect.backoff.ms",    String.valueOf(reconnectBackoffMs));
        props.put("reconnect.backoff.max.ms",String.valueOf(reconnectBackoffMs));
        props.put("retry.backoff.ms",        String.valueOf(endpoint.getMaxRetryBackoffMs()));

        // Manual commit: CPI processing success must be confirmed before advancing the offset.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                normalizeAutoOffsetReset(endpoint.getAutoOffsetReset()));
    }

    // =========================================================================
    // Record processing
    // =========================================================================

    /**
     * Constructs a Camel {@link Exchange} from a Kafka {@link ConsumerRecord}.
     *
     * <p>Hotpath notes:
     * <ul>
     *   <li>The payload byte array is passed by reference — no copy.</li>
     *   <li>The hex preview header uses the pre-allocated {@link #HEX} array
     *       and a pre-sized {@code StringBuilder} to avoid per-record allocation.</li>
     *   <li>Conversion dispatching (Avro/JSON/XML/pass-through) is handled via
     *       string equality checks, not regex or enum, to keep branch cost minimal.</li>
     * </ul>
     */
    private Exchange processRecord(final ConsumerRecord<String, byte[]> record) {
        final Exchange exchange = endpoint.createExchange();

        exchange.getIn().setHeader("x-sdiakafka-topic",     record.topic());
        exchange.getIn().setHeader("x-sdiakafka-partition", record.partition());
        exchange.getIn().setHeader("x-sdiakafka-offset",    record.offset());
        exchange.getIn().setHeader("x-sdiakafka-key",       record.key());
        exchange.getIn().setHeader("x-sdiakafka-timestamp", record.timestamp());

        if (endpoint.isMessageRecoveryEnabled()) {
            final TopicPartition recoveryTp = new TopicPartition(record.topic(), record.partition());
            exchange.getIn().setHeader("x-sdiakafka-recovery-mode",                     endpoint.getRecoveryMode());
            exchange.getIn().setHeader("x-sdiakafka-seek-input",                         recoverySeekInput);
            exchange.getIn().setHeader("x-sdiakafka-seek-timestamp-ms",                  recoverySeekTimestampMs);
            exchange.getIn().setHeader("x-sdiakafka-seek-applied",                       recoverySeekApplied);
            exchange.getIn().setHeader("x-sdiakafka-seek-offset",                        recoverySeekOffsets.get(recoveryTp));
            exchange.getIn().setHeader("x-sdiakafka-recovery-read-mode",                 endpoint.getRecoveryReadMode());
            exchange.getIn().setHeader("x-sdiakafka-recovery-single-message-completed",  recoverySingleMessageCompleted);
        }

        final byte[] payloadBytes  = record.value();
        final int    payloadLength = (payloadBytes == null) ? 0 : payloadBytes.length;

        // Hex preview: reuse the pre-allocated instance field — zero GC pressure per record.
        final int previewLimit = Math.min(payloadLength, endpoint.getEffectiveSchemaIdSizeBuffer());
        payloadHexPreview.setLength(0); // clear without re-allocating the backing array
        if (payloadBytes != null && previewLimit > 0) {
            for (int i = 0; i < previewLimit; i++) {
                appendHexByte(payloadHexPreview, payloadBytes[i]);
                if (i < previewLimit - 1) {
                    payloadHexPreview.append(' ');
                }
            }
        } else {
            payloadHexPreview.append("EMPTY");
        }
        exchange.getIn().setHeader("x-sdiakafka-payload-first-bytes-hex", payloadHexPreview.toString());

        final String conversionFormat      = normalizeConversionFormat(endpoint.getEffectiveConversionFormat());
        final String schemaRegistryUrl     = trimToNull(endpoint.getSchemaRegistryHostAddress());
        final String registryCredAlias     = trimToNull(endpoint.getSchemaRegistryCredentialAlias());

        final boolean conversionRequested  = !"None".equalsIgnoreCase(conversionFormat)
                && !"JSON_SCHEMA".equalsIgnoreCase(conversionFormat)
                && !"PROTOBUF".equalsIgnoreCase(conversionFormat);

        if ("JSON_SCHEMA".equalsIgnoreCase(conversionFormat)) {
            exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/json");
            exchange.getIn().setHeader("x-sdiakafka-convertionFormat", "JSON_SCHEMA");
            exchange.getIn().setBody(payloadBytes != null
                    ? new String(payloadBytes, StandardCharsets.UTF_8) : "");
            return exchange;
        }

        if ("PROTOBUF".equalsIgnoreCase(conversionFormat)) {
            return markError(exchange, payloadBytes,
                    "Event.Smart.Kafka.Protobuf.NotImplemented",
                    "PROTOBUF", "n/a", -1,
                    "PROTOBUF conversion is reserved for a future release. Select None, JSON, or XML.");
        }

        exchange.getIn().setHeader("x-sdiakafka-convertionFormat",  conversionRequested ? conversionFormat : "None");
        exchange.getIn().setHeader("x-sdiakafka-conversionFormat",   conversionRequested ? conversionFormat : "None");

        if (conversionRequested) {
            final long maxPayloadBytes = endpoint.getMaxPayloadSizeBytes();
            if (payloadLength > maxPayloadBytes) {
                return markError(exchange, payloadBytes,
                        "Event.Smart.Kafka.Payload.TooLarge", conversionFormat, "n/a", -1,
                        "Payload size " + payloadLength + " bytes exceeds the configured Avro conversion limit of "
                                + endpoint.getMaxPayloadSizeMb() + " MB."
                                + " For larger events, use external storage and send a pointer event instead.");
            }

            if (schemaRegistryUrl == null) {
                return markError(exchange, payloadBytes,
                        "Event.Smart.Kafka.Config.Missing.RegistryHost", conversionFormat, "n/a", -1,
                        "Schema Registry Host Address is mandatory.");
            }
            if (registryCredAlias == null) {
                return markError(exchange, payloadBytes,
                        "Event.Smart.Kafka.Config.Missing.Credential", conversionFormat, "n/a", -1,
                        "Schema Registry Credential Alias is mandatory.");
            }
            if (payloadBytes == null || payloadLength == 0) {
                return markError(exchange, payloadBytes,
                        "Event.Smart.Kafka.Empty.Payload", conversionFormat, "n/a", -1,
                        "Payload is empty.");
            }

            int    resolvedSchemaId    = -1;
            String effectiveSchemaMode = "Unresolved";

            try {
                final String  fixedSchemaIdStr     = trimToNull(endpoint.getSchemaRegistrySchemaId());
                final boolean schemaIdFromMagicByte = shouldUseMagicByte(endpoint.getSchemaIdResolutionMode(), fixedSchemaIdStr, payloadBytes);

                int     schemaId         = 0;
                Integer fixedSchemaIdObj = null;

                if (schemaIdFromMagicByte) {
                    schemaId         = SdiaKafkaAvroConverter.extractSchemaId(record.value());
                    effectiveSchemaMode = "MagicByte";
                } else {
                    if (fixedSchemaIdStr != null) {
                        try {
                            fixedSchemaIdObj = Integer.valueOf(fixedSchemaIdStr);
                            schemaId         = fixedSchemaIdObj.intValue();
                        } catch (NumberFormatException nfe) {
                            return markError(exchange, payloadBytes,
                                    "Event.Smart.Kafka.Invalid.SchemaID.Format", conversionFormat, "FixedSchemaID", -1,
                                    "Fixed Schema ID must be a positive numeric value. Configured value: " + fixedSchemaIdStr);
                        }
                        effectiveSchemaMode = "FixedSchemaID";
                    } else {
                        throw new IllegalArgumentException(
                                "CONFIG ERROR: Schema ID resolution failed. "
                                + "The message does not contain a Magic Byte header, and no fallback Fixed Schema ID is defined on the CPI Channel.");
                    }
                }
                resolvedSchemaId = schemaId;

                final SdiaKafkaCredentials credentials = SdiaKafkaCredentialsResolver.resolve(registryCredAlias);

                final String converted = SdiaKafkaAvroConverter.convertWithTtl(
                        payloadBytes,
                        conversionFormat,
                        schemaRegistryUrl,
                        credentials.getUsername(),
                        credentials.getPassword(),
                        schemaIdFromMagicByte,
                        fixedSchemaIdObj != null ? fixedSchemaIdObj : Integer.valueOf(resolvedSchemaId),
                        endpoint.getSchemaCacheMaxBytes(),
                        endpoint.getSchemaCacheTtlMs());

                exchange.getIn().setHeader("SAP_KafkaSchemaId",                      schemaId);
                exchange.getIn().setHeader("SAP_KafkaSchemaRegistryUrl",              schemaRegistryUrl);
                exchange.getIn().setHeader("SAP_KafkaSchemaRegistryCredentialAlias",  registryCredAlias);
                exchange.getIn().setHeader("x-sdiakafka-schema-mode",                 effectiveSchemaMode);
                exchange.getIn().setHeader(Exchange.CONTENT_TYPE,
                        "XML".equalsIgnoreCase(conversionFormat) ? "application/xml" : "application/json");
                exchange.getIn().setBody(converted);

            } catch (Throwable t) {
                String errorCode;
                String rootMsg = rootCauseMessage(t);
                if (rootMsg != null && rootMsg.contains("body=")) {
                    rootMsg = rootMsg.substring(rootMsg.indexOf("body="));
                }
                final String m = (rootMsg == null ? "" : rootMsg.toLowerCase());
                if (m.contains("schema not found") || m.contains("404")) {
                    errorCode = "Event.Smart.Kafka.Avro.Registry.NotFound";
                } else if (m.contains("authentication") || m.contains("401") || m.contains("403")) {
                    errorCode = "Event.Smart.Kafka.Avro.Registry.Auth.Failure";
                } else if (m.contains("magic byte")) {
                    errorCode = "Event.Smart.Kafka.Invalid.SchemaID.Format";
                } else if (m.contains("connect") || m.contains("timeout")) {
                    errorCode = "Event.Smart.Kafka.Avro.Registry.Network.Failure";
                } else {
                    errorCode = "Event.Smart.Kafka.Avro.Conversion.Failure";
                }
                return markError(exchange, payloadBytes, errorCode, conversionFormat, effectiveSchemaMode, resolvedSchemaId, rootMsg);
            }

        } else {
            // Raw pass-through — guard against unexpectedly large records.
            if (payloadLength > SdiaKafkaEndpoint.RAW_PAYLOAD_HARD_LIMIT_BYTES) {
                return markError(exchange, payloadBytes,
                        "Event.Smart.Kafka.Payload.RawTooLarge",
                        "None", "n/a", -1,
                        "Raw payload size " + payloadLength + " bytes exceeds the adapter hard limit of 20 MB. "
                                + "Send a pointer event instead of embedding large payloads in Kafka records.");
            }
            exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/octet-stream");
            exchange.getIn().setBody(payloadBytes);
        }

        return exchange;
    }

    // =========================================================================
    // Pipeline delivery with retry / Stop on Error
    // =========================================================================

    private void processPipelineExchangeWithPolicy(final Exchange exchange) throws Exception {
        final String handling = normalizeErrorHandling(endpoint.getErrorHandling());

        if (!"Retry Failed Message".equalsIgnoreCase(handling)) {
            getProcessor().process(exchange);
            final Exception processorEx = exchange.getException();
            if (processorEx != null) {
                throw processorEx;
            }
            return;
        }

        int retries = endpoint.getRetryAttempts() != null ? endpoint.getRetryAttempts().intValue() : 5;
        if (retries < 5)  { retries = 5;  }
        if (retries > 10) { retries = 10; }
        final int totalAttempts = retries + 1;

        Throwable lastFailure = null;
        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                exchange.setException(null);
                getProcessor().process(exchange);
                final Exception processorEx = exchange.getException();
                if (processorEx != null) {
                    throw processorEx;
                }
                if (attempt > 1) {
                    LOG.warn("[SDIA Kafka] Pipeline recovered on attempt {}/{}.", attempt, totalAttempts);
                }
                return;
            } catch (Throwable pipelineEx) {
                lastFailure = pipelineEx;
                if (attempt >= totalAttempts) {
                    break;
                }
                final long delaySec = Math.min(30L, (long) Math.pow(2, attempt - 1));
                LOG.warn("[SDIA Kafka] Pipeline attempt " + attempt + "/" + totalAttempts
                        + " failed. Retrying in " + delaySec + "s. Detail: " + rootCauseMessage(pipelineEx));
                try {
                    Thread.sleep(delaySec * 1_000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeCamelException("Retry interrupted", pipelineEx);
                }
            }
        }

        LOG.error("[SDIA Kafka] All {} retry attempts exhausted. Offset NOT committed — record re-delivered after restart.", retries);
        if (lastFailure instanceof Exception) {
            throw (Exception) lastFailure;
        }
        throw new RuntimeCamelException(lastFailure);
    }

    // =========================================================================
    // Offset management
    // =========================================================================

    private void commitSuccessOffsets(final Map<TopicPartition, OffsetAndMetadata> successOffsets,
                                      final int delivered) {
        if (successOffsets == null || successOffsets.isEmpty() || closed.get() || kafkaConsumer == null) {
            return;
        }

        try {
            // No defensive copy needed — commitSync is synchronous and the consumer
            // is single-threaded. The map is not accessed concurrently.
            kafkaConsumer.commitSync(successOffsets, Duration.ofSeconds(10));
        } catch (Throwable commitEx) {
            final String errorMsg = buildVisualError(
                    "Kafka Offset Commit Failure.",
                    "Consumer Group: "  + effectiveGroupId
                            + "\nCommit Offsets: " + formatOffsets(successOffsets)
                            + "\nError detail: "   + rootCauseMessage(commitEx),
                    "The consumer will be recycled and retried. "
                            + "Check group.id stability, ACL permissions, session timeout, "
                            + "max.poll.interval.ms, and consumer group authorisation.");
            LOG.error("[SDIA Kafka] {}", errorMsg, commitEx);
            try { surfaceErrorToChannel(errorMsg); } catch (Throwable ignored) {}

            closeKafkaConsumerSilently();
            initialized              = false;
            fatalInitFailure         = true;
            fatalInitReason          = errorMsg;
            nextInitRetryAllowedAtMs = System.currentTimeMillis() + INIT_FAILURE_RETRY_BACKOFF_MS;

            throw new RuntimeCamelException(errorMsg, commitEx);
        }
    }

    private void commitAvroPoisonOffsetThenRecycle(final ConsumerRecord<String, byte[]> record,
                                                   final String fatalMsg,
                                                   final Throwable conversionEx) {
        final TopicPartition tp = new TopicPartition(record.topic(), record.partition());
        final Map<TopicPartition, OffsetAndMetadata> poisonCommit = new LinkedHashMap<>();
        poisonCommit.put(tp, new OffsetAndMetadata(record.offset() + 1));

        LOG.error("[SDIA Kafka] {}", fatalMsg, conversionEx);
        try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), fatalMsg); } catch (Throwable ignored) {}

        try {
            kafkaConsumer.commitSync(poisonCommit, Duration.ofSeconds(10));
            LOG.warn("[SDIA Kafka] Avro poison pill committed and skipped. group.id=" + effectiveGroupId
                    + " topic=" + record.topic()
                    + " partition=" + record.partition()
                    + " failedOffset=" + record.offset()
                    + " committedOffset=" + (record.offset() + 1));

            if (shouldStopAfterRecoveredRecord(record)) {
                restoreOriginalCommittedOffsetsAfterSingleRecovery(record, "skipped after conversion failure");
            }
        } catch (Throwable commitEx) {
            final String commitMsg = buildVisualError(
                    "Kafka Offset Commit Failure after Avro poison pill.",
                    "Consumer Group: "    + effectiveGroupId
                            + "\nRecord Topic: "    + record.topic()
                            + "\nTarget Offset: "   + record.offset()
                            + "\nTarget Partition: "+ record.partition()
                            + "\nCommit Offset: "   + (record.offset() + 1)
                            + "\nError detail: "    + rootCauseMessage(commitEx),
                    "The poison-pill offset was NOT committed. The same record will be retried. Verify ACLs and group.id.");
            LOG.error("[SDIA Kafka] {}", commitMsg, commitEx);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), commitMsg); } catch (Throwable ignored) {}
            throw new RuntimeCamelException(commitMsg, commitEx);
        } finally {
            // Force clean restart from committed offset + 1.
            initialized                = false;
            fatalAvroConversionFailure = false;
            fatalAvroConversionReason  = null;
            nextAvroRetryAllowedAtMs   = 0L;
            closeKafkaConsumerSilently();
        }
    }

    // =========================================================================
    // Error policy halting
    // =========================================================================

    private void haltConsumerAfterFatalAvroFailure(final ConsumerRecord<String, byte[]> record,
                                                   final String reason,
                                                   final Throwable cause) {
        try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), reason); } catch (Throwable ignored) {}

        final String handling = normalizeErrorHandling(endpoint.getErrorHandling());

        if ("Stop on Error".equalsIgnoreCase(handling)) {
            stoppedByErrorPolicy       = true;
            stoppedByErrorPolicyReason = reason;
            fatalAvroConversionFailure = false;
            fatalAvroConversionReason  = null;
            nextAvroRetryAllowedAtMs   = 0L;
            stopConsumerForStopOnError(record, cause, "Avro conversion failure");
            return;
        }

        // Retry / default: seek back to the same offset and wait for the backoff.
        fatalAvroConversionFailure = true;
        fatalAvroConversionReason  = reason;
        nextAvroRetryAllowedAtMs   = System.currentTimeMillis() + AVRO_FAILURE_RETRY_BACKOFF_MS;

        if (kafkaConsumer != null && record != null) {
            try {
                kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                LOG.warn("[SDIA Kafka] Retry after Avro failure — seeked back to offset " + record.offset()
                        + " on " + record.topic() + "-" + record.partition()
                        + ". Next retry in " + AVRO_FAILURE_RETRY_BACKOFF_MS + " ms.");
            } catch (Throwable seekEx) {
                LOG.warn("[SDIA Kafka] Could not seek back after Avro failure: {}", rootCauseMessage(seekEx));
            }
        }
    }

    private void haltConsumerAfterPipelineFailure(final ConsumerRecord<String, byte[]> record,
                                                  final String reason,
                                                  final Throwable cause) {
        try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), reason); } catch (Throwable ignored) {}

        final String handling = normalizeErrorHandling(endpoint.getErrorHandling());

        if ("Stop on Error".equalsIgnoreCase(handling)) {
            stoppedByErrorPolicy       = true;
            stoppedByErrorPolicyReason = reason;
            stopConsumerForStopOnError(record, cause, "Pipeline processor failure");
            return;
        }

        initialized = false;

        if (kafkaConsumer != null && record != null) {
            try {
                kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                LOG.warn("[SDIA Kafka] Pipeline failure retry — seeked back to offset " + record.offset()
                        + " on " + record.topic() + "-" + record.partition() + ".");
            } catch (Throwable seekEx) {
                LOG.warn("[SDIA Kafka] Could not seek back after pipeline failure: {}", rootCauseMessage(seekEx));
            }
        }
        closeKafkaConsumerSilently();
    }

    private void stopConsumerForStopOnError(final ConsumerRecord<String, byte[]> record,
                                            final Throwable cause,
                                            final String failureType) {
        try {
            if (kafkaConsumer != null && record != null) {
                try {
                    kafkaConsumer.seek(new TopicPartition(record.topic(), record.partition()), record.offset());
                } catch (Throwable seekEx) {
                    LOG.warn("[SDIA Kafka] Stop on Error could not seek back before shutdown: {}", rootCauseMessage(seekEx));
                }
                LOG.error("[SDIA Kafka] Stop on Error activated. Offset NOT committed."
                        + " KafkaConsumer will be closed; adapter silent until redeploy."
                        + " failureType=" + failureType
                        + " topic=" + record.topic()
                        + " partition=" + record.partition()
                        + " failedOffset=" + record.offset(),
                        cause);
            } else {
                LOG.error("[SDIA Kafka] Stop on Error activated without active KafkaConsumer/record. "
                        + "Offset NOT committed. Adapter stopped until redeploy.", cause);
            }
        } catch (Throwable stopEx) {
            LOG.warn("[SDIA Kafka] Stop on Error shutdown handler failed; adapter remains logically stopped: {}",
                    rootCauseMessage(stopEx));
        } finally {
            initialized = false;
            closeKafkaConsumerSilently();
        }
    }

    private void publishStopErrorExchangeToIFlow(final Exchange exchange,
                                                  final ConsumerRecord<String, byte[]> record,
                                                  final String fatalMsg,
                                                  final Throwable cause,
                                                  final String failureType) {
        if (exchange == null) {
            LOG.warn("[SDIA Kafka] Stop on Error visibility skipped — exchange is null. failedOffset={}",
                    record == null ? "<unknown>" : record.offset());
            return;
        }
        try {
            exchange.getIn().setHeader("x-sdiakafka-error-handling",   "Stop on Error");
            exchange.getIn().setHeader("x-sdiakafka-stop-on-error",    Boolean.TRUE);
            exchange.getIn().setHeader("x-sdiakafka-stop-failure-type", failureType);
            exchange.getIn().setHeader("x-sdiakafka-offset-committed",  Boolean.FALSE);
            exchange.getIn().setHeader("x-sdiakafka-next-offset-read",  Boolean.FALSE);
            exchange.getIn().setHeader("x-sdiakafka-error-message",     fatalMsg);
            exchange.setException(new RuntimeCamelException(fatalMsg, cause));

            LOG.error("[SDIA Kafka] Stop on Error visible failure exchange published."
                    + " Offset NOT committed."
                    + " group.id=" + effectiveGroupId
                    + " topic=" + (record == null ? "<unknown>" : record.topic())
                    + " partition=" + (record == null ? "<unknown>" : record.partition())
                    + " failedOffset=" + (record == null ? "<unknown>" : record.offset()));

            try {
                getProcessor().process(exchange);
            } catch (Throwable routeEx) {
                LOG.warn("[SDIA Kafka] Stop on Error exchange delivered as failure; downstream returned exception. "
                        + "Offset NOT committed. Detail: {}", rootCauseMessage(routeEx));
            }
        } catch (Throwable ex) {
            LOG.warn("[SDIA Kafka] Stop on Error visibility publication failed. Offset NOT committed. Detail: {}",
                    rootCauseMessage(ex));
        }
    }

    // =========================================================================
    // Message Recovery
    // =========================================================================

    private boolean shouldStopAfterRecoveredRecord(final ConsumerRecord<String, byte[]> record) {
        if (record == null || !endpoint.isMessageRecoveryEnabled() || !endpoint.isRecoverySingleMessageOnly()) {
            return false;
        }
        final Long seekOffset = recoverySeekOffsets.get(new TopicPartition(record.topic(), record.partition()));
        return recoverySeekApplied && seekOffset != null && record.offset() == seekOffset.longValue();
    }

    private void markRecoverySingleMessageCompleted(final ConsumerRecord<String, byte[]> record,
                                                    final String result) {
        recoverySingleMessageCompleted  = true;
        if (record != null) {
            recoveryCompletedOffset         = record.offset();
            recoveryCompletedTopicPartition = record.topic() + "-" + record.partition();
        }
        LOG.warn("[SDIA Kafka] Message Recovery single-record mode completed."
                + " Adapter will not consume the next offset until recovery is disabled and iFlow redeployed."
                + " result=" + result
                + " topicPartition=" + recoveryCompletedTopicPartition
                + " recoveredOffset=" + recoveryCompletedOffset
                + " recoveryInput=" + recoverySeekInput
                + " timestampMs=" + recoverySeekTimestampMs
                + " officialGroupOffsetPreserved=true");
    }

    private void restoreOriginalCommittedOffsetsAfterSingleRecovery(final ConsumerRecord<String, byte[]> record,
                                                                     final String result) {
        if (!endpoint.isMessageRecoveryEnabled() || !endpoint.isRecoverySingleMessageOnly()) {
            return;
        }
        if (kafkaConsumer == null) {
            return;
        }
        if (recoveryOriginalCommittedOffsets.isEmpty()) {
            LOG.warn("[SDIA Kafka] Message Recovery single-record: no original committed offset existed for restore."
                    + " result=" + result
                    + " topic=" + (record == null ? "<unknown>" : record.topic())
                    + " partition=" + (record == null ? -1 : record.partition())
                    + " recoveredOffset=" + (record == null ? -1L : record.offset()));
            return;
        }
        try {
            final Map<TopicPartition, OffsetAndMetadata> snapshot = new LinkedHashMap<>(recoveryOriginalCommittedOffsets);
            LOG.warn("[SDIA Kafka] Message Recovery restoring original committed offsets."
                    + " Recovered record will NOT become official consumer-group position."
                    + " result=" + result
                    + " recoveredTopic=" + (record == null ? "<unknown>" : record.topic())
                    + " recoveredPartition=" + (record == null ? -1 : record.partition())
                    + " recoveredOffset=" + (record == null ? -1L : record.offset())
                    + " restoringOffsets=" + formatOffsets(snapshot));

            kafkaConsumer.commitSync(snapshot, Duration.ofSeconds(10));

            LOG.warn("[SDIA Kafka] Original committed offsets restored. restoredOffsets={}", formatOffsets(snapshot));
        } catch (Throwable restoreEx) {
            final String msg = buildVisualError(
                    "Message Recovery Offset Restoration Failure.",
                    "Recovered record was processed, but the adapter could not restore the original committed offset."
                            + "\nConsumer Group: "     + effectiveGroupId
                            + "\nRecovery Input: "     + recoverySeekInput
                            + "\nRecovery Timestamp ms: " + recoverySeekTimestampMs
                            + "\nOriginal Offsets: "   + formatOffsets(recoveryOriginalCommittedOffsets)
                            + "\nError detail: "       + rootCauseMessage(restoreEx),
                    "Do not disable Message Recovery yet. Verify Kafka consumer group commit permissions and retry, "
                            + "or manually reset the consumer group offset in Kafka/Confluent.");
            LOG.error("[SDIA Kafka] {}", msg, restoreEx);
            throw new RuntimeCamelException(msg, restoreEx);
        }
    }

    private String applyMessageRecoverySeekIfConfigured(final List<String> topicEntries,
                                                         final Map<String, List<PartitionInfo>> knownTopics,
                                                         final String partitionsParam) {
        final long seekTs = endpoint.getSeekToTimestampMs();
        if (!endpoint.isMessageRecoveryEnabled() || seekTs <= 0L || kafkaConsumer == null) {
            recoverySeekApplied      = false;
            recoverySeekTimestampMs  = -1L;
            recoverySeekInput        = null;
            recoverySeekOffsets.clear();
            return null;
        }

        recoverySeekApplied     = false;
        recoverySeekTimestampMs = seekTs;
        recoverySeekInput       = endpoint.getRecoveryTimestampValue();
        recoverySeekOffsets.clear();

        try {
            final List<TopicPartition> recoveryPartitions =
                    resolveRecoveryTopicPartitions(topicEntries, knownTopics, partitionsParam);

            if (recoveryPartitions == null || recoveryPartitions.isEmpty()) {
                return buildVisualError(
                        "Message Recovery seek could not resolve Kafka partitions.",
                        "Recovery Mode: " + endpoint.getRecoveryMode()
                                + "\nTimestamp Input: " + recoverySeekInput
                                + "\nTimestamp ms: "    + seekTs
                                + "\nTopic Expression: " + endpoint.getTopicPattern()
                                + "\nPartitions: "       + partitionsParam,
                        "1 - Verify the topic exists."
                                + "\n2 - If using a wildcard/pattern, verify it matches existing topics."
                                + "\n3 - If using explicit partitions, verify the partition numbers exist.");
            }

            captureOriginalCommittedOffsetsForSingleRecovery(recoveryPartitions);

            try { kafkaConsumer.unsubscribe(); } catch (Throwable ignored) {}
            kafkaConsumer.assign(recoveryPartitions);
            assignMode = true;

            final Map<TopicPartition, Long> timestamps = new LinkedHashMap<>();
            for (final TopicPartition tp : recoveryPartitions) {
                timestamps.put(tp, seekTs);
            }

            final Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsets =
                    kafkaConsumer.offsetsForTimes(timestamps);

            int applied = 0;
            final StringBuilder noOffsetPartitions = new StringBuilder();

            for (final TopicPartition tp : recoveryPartitions) {
                final org.apache.kafka.clients.consumer.OffsetAndTimestamp oat =
                        offsets != null ? offsets.get(tp) : null;

                if (oat != null) {
                    kafkaConsumer.seek(tp, oat.offset());
                    recoverySeekOffsets.put(tp, oat.offset());
                    applied++;
                    LOG.warn("[SDIA Kafka] Message Recovery seek applied. topic=" + tp.topic()
                            + " partition=" + tp.partition()
                            + " timestampMs=" + seekTs
                            + " offset=" + oat.offset());
                } else {
                    if (noOffsetPartitions.length() > 0) {
                        noOffsetPartitions.append(", ");
                    }
                    noOffsetPartitions.append(tp.topic()).append('-').append(tp.partition());
                    try {
                        kafkaConsumer.seekToEnd(Collections.singletonList(tp));
                        recoverySeekOffsets.put(tp, kafkaConsumer.position(tp));
                    } catch (Throwable seekEndEx) {
                        LOG.warn("[SDIA Kafka] Message Recovery could not seekToEnd for {}: {}",
                                tp, rootCauseMessage(seekEndEx));
                    }
                }
            }

            if (applied <= 0) {
                return buildVisualError(
                        "Message Recovery seek found no offset for timestamp.",
                        "Recovery Mode: "         + endpoint.getRecoveryMode()
                                + "\nTimestamp Input: "   + recoverySeekInput
                                + "\nTimestamp ms: "      + seekTs
                                + "\nNo offset partitions: " + noOffsetPartitions
                                + "\nTopic Expression: "  + endpoint.getTopicPattern(),
                        "1 - Use a timestamp that exists inside the Kafka topic retention window."
                                + "\n2 - Copy the timestamp from Confluent and try one or two seconds earlier."
                                + "\n3 - Confirm whether the topic uses CreateTime or LogAppendTime timestamps."
                                + "\n4 - Disable Message Recovery after replay.");
            }

            recoverySeekApplied = true;
            LOG.warn("[SDIA Kafka] Message Recovery seek completed."
                    + " recoveryInput=" + recoverySeekInput
                    + " timestampMs=" + seekTs
                    + " requestedPartitions=" + recoveryPartitions.size()
                    + " appliedPartitions=" + applied
                    + " noOffsetPartitions=" + noOffsetPartitions
                    + " assignMode=true");
            return null;

        } catch (WakeupException wakeupEx) {
            throw wakeupEx;
        } catch (Throwable seekEx) {
            recoverySeekApplied = false;
            return buildVisualError(
                    "Message Recovery seek failed.",
                    "Recovery Input: " + recoverySeekInput
                            + "\nTimestamp ms: " + seekTs
                            + "\nError: "        + rootCauseMessage(seekEx),
                    "1 - Verify timestamp format."
                            + "\n2 - Verify topic, partitions and permissions."
                            + "\n3 - Disable Message Recovery if normal consumption is required.");
        }
    }

    private void captureOriginalCommittedOffsetsForSingleRecovery(final List<TopicPartition> recoveryPartitions) {
        recoveryOriginalCommittedOffsets.clear();
        if (!endpoint.isMessageRecoveryEnabled() || !endpoint.isRecoverySingleMessageOnly()) {
            return;
        }
        if (kafkaConsumer == null || recoveryPartitions == null || recoveryPartitions.isEmpty()) {
            return;
        }
        for (final TopicPartition tp : recoveryPartitions) {
            if (tp == null) {
                continue;
            }
            try {
                final OffsetAndMetadata committed = kafkaConsumer.committed(tp);
                if (committed != null) {
                    recoveryOriginalCommittedOffsets.put(tp, committed);
                }
            } catch (Throwable ex) {
                LOG.warn("[SDIA Kafka] Message Recovery could not read original committed offset."
                        + " group.id=" + effectiveGroupId
                        + " topic=" + tp.topic()
                        + " partition=" + tp.partition()
                        + " error=" + rootCauseMessage(ex));
            }
        }
        LOG.warn("[SDIA Kafka] Message Recovery captured original committed offsets. group.id={} offsets={}",
                effectiveGroupId, formatOffsets(recoveryOriginalCommittedOffsets));
    }

    private List<TopicPartition> resolveRecoveryTopicPartitions(final List<String> topicEntries,
                                                                 final Map<String, List<PartitionInfo>> knownTopics,
                                                                 final String partitionsParam) {
        final List<TopicPartition> result = new ArrayList<>();
        if (topicEntries == null || topicEntries.isEmpty()) {
            return result;
        }

        if (partitionsParam != null && !partitionsParam.trim().isEmpty()) {
            if (topicEntries.size() == 1 && !isPatternMode(topicEntries.get(0))) {
                return parsePartitions(topicEntries.get(0), partitionsParam);
            }
            return result;
        }

        final List<String> resolvedTopics = new ArrayList<>();
        if (containsPatternMode(topicEntries)) {
            final Pattern p = compileTopicPatternExpression(topicEntries);
            if (knownTopics != null) {
                for (final String topic : knownTopics.keySet()) {
                    if (topic != null && p.matcher(topic).matches()) {
                        resolvedTopics.add(topic);
                    }
                }
            }
        } else {
            resolvedTopics.addAll(topicEntries);
        }

        for (final String topic : resolvedTopics) {
            if (topic == null || topic.trim().isEmpty()) {
                continue;
            }
            List<PartitionInfo> partitions = (knownTopics != null) ? knownTopics.get(topic) : null;
            if (partitions == null || partitions.isEmpty()) {
                try {
                    partitions = kafkaConsumer.partitionsFor(topic);
                } catch (Throwable ex) {
                    LOG.warn("[SDIA Kafka] Message Recovery could not lookup partitions for topic={} error={}",
                            topic, rootCauseMessage(ex));
                    partitions = null;
                }
            }
            if (partitions == null || partitions.isEmpty()) {
                continue;
            }
            for (final PartitionInfo pi : partitions) {
                if (pi != null) {
                    result.add(new TopicPartition(pi.topic(), pi.partition()));
                }
            }
        }
        return result;
    }

    // =========================================================================
    // Assigned-partition start-offset positioning
    // =========================================================================

    private void applyAssignedStartOffsets(final List<TopicPartition> topicPartitions,
                                           final String reason) {
        if (kafkaConsumer == null || topicPartitions == null || topicPartitions.isEmpty()) {
            return;
        }
        final String reset = normalizeAutoOffsetReset(endpoint.getAutoOffsetReset());
        final Map<TopicPartition, Long> applied = new LinkedHashMap<>();
        final List<TopicPartition>      noCommit = new ArrayList<>();

        try {
            for (final TopicPartition tp : topicPartitions) {
                final OffsetAndMetadata committed = kafkaConsumer.committed(tp);
                if (committed != null) {
                    kafkaConsumer.seek(tp, committed.offset());
                    applied.put(tp, committed.offset());
                } else {
                    noCommit.add(tp);
                }
            }

            if (!noCommit.isEmpty()) {
                if ("earliest".equalsIgnoreCase(reset)) {
                    kafkaConsumer.seekToBeginning(noCommit);
                    for (final TopicPartition tp : noCommit) {
                        applied.put(tp, kafkaConsumer.position(tp));
                    }
                } else if ("latest".equalsIgnoreCase(reset)) {
                    kafkaConsumer.seekToEnd(noCommit);
                    for (final TopicPartition tp : noCommit) {
                        applied.put(tp, kafkaConsumer.position(tp));
                    }
                } else {
                    throw new IllegalStateException(
                            "auto.offset.reset=none and no committed offset exists for " + noCommit
                            + ". Restore the committed offset for group.id=" + effectiveGroupId
                            + " or select EARLIEST/LATEST.");
                }
            }

            LOG.warn("[SDIA Kafka] Assigned consumer start position applied."
                    + " reason=" + reason
                    + " group.id=" + effectiveGroupId
                    + " assignedPartitions=" + topicPartitions
                    + " positions=" + applied
                    + " auto.offset.reset=" + reset
                    + " committedUsed=" + (topicPartitions.size() - noCommit.size())
                    + " resetApplied=" + noCommit.size()
                    + " onPremise=" + isOnPremiseProxyMode());

        } catch (Throwable t) {
            throw new RuntimeCamelException(
                    "Assigned start offset positioning failed. reason=" + reason
                    + " | auto.offset.reset=" + reset
                    + " | partitions=" + topicPartitions
                    + " | group.id=" + effectiveGroupId
                    + " | detail=" + rootCauseMessage(t), t);
        }
    }

    // =========================================================================
    // Heartbeat / diagnostics
    // =========================================================================

    private void logEmptyPollHeartbeat() {
        final long now = System.currentTimeMillis();
        if (now - lastEmptyPollLogAtMs < EMPTY_POLL_DIAG_LOG_EVERY_MS) {
            return;
        }
        lastEmptyPollLogAtMs = now;

        try {
            final java.util.Set<TopicPartition> assignment   = (kafkaConsumer != null)
                    ? kafkaConsumer.assignment()   : Collections.<TopicPartition>emptySet();
            final java.util.Set<String>         subscription = (kafkaConsumer != null)
                    ? kafkaConsumer.subscription() : Collections.<String>emptySet();
            LOG.warn("[SDIA Kafka] EMPTY POLL heartbeat."
                    + " group.id=" + effectiveGroupId
                    + " initialized=" + initialized
                    + " subscription=" + subscription
                    + " assignment=" + assignment
                    + " topicPattern=" + endpoint.getTopicPattern());
        } catch (Throwable t) {
            LOG.warn("[SDIA Kafka] EMPTY POLL heartbeat failed: {}", rootCauseMessage(t));
        }
    }

    private void surfaceErrorToChannel(final String errorMessage) {
        try {
            final Exchange errorExchange = endpoint.createExchange();
            final String fullMsg = isVisualAdapterError(errorMessage)
                    ? errorMessage
                    : buildVisualError(
                            "Event Smart Kafka — Adapter Initialisation Failure",
                            "Bootstrap: [" + endpoint.resolveBootstrapServers() + "]"
                                    + "\nTopic: ["      + endpoint.getTopicPattern()      + "]"
                                    + "\nCredential: [" + endpoint.getCredentialAlias()   + "]"
                                    + "\n\n" + errorMessage,
                            "Review the adapter configuration and redeploy the iFlow.");
            errorExchange.setException(new RuntimeCamelException(fullMsg));
            try {
                getProcessor().process(errorExchange);
            } catch (Throwable ignored) {
                getExceptionHandler().handleException("Adapter init failure", errorExchange,
                        new RuntimeCamelException(fullMsg));
            }
        } catch (Throwable t) {
            LOG.error("[SDIA Kafka] surfaceErrorToChannel failed: {}", t.getMessage());
        }
    }

    // =========================================================================
    // On-Premise Cloud Connector tunnel
    // =========================================================================

    private boolean isOnPremiseProxyMode() {
        final String proxy = trimToNull(endpoint.getProxyType());
        if (proxy == null) {
            return false;
        }
        final String n = proxy.replace('_', ' ').replace('-', ' ').toLowerCase();
        return n.contains("on premise") || n.contains("onpremise");
    }

    private void applyOnPremiseClientBootstrapIfRequired(final Properties props,
                                                          final String kafkaClientBootstrap) {
        if (!isOnPremiseProxyMode() || props == null) {
            return;
        }
        final String bs = trimToNull(kafkaClientBootstrap);
        if (bs != null) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bs);
            props.put("metadata.max.age.ms", "10000");
        }
    }

    private synchronized String ensureOnPremiseTcpTunnel(final String virtualBootstrapServers) throws Exception {
        final String current = trimToNull(onPremiseClientBootstrapServers);
        if (onPremiseTcpTunnel != null && onPremiseTcpTunnel.isRunning() && current != null) {
            return current;
        }
        closeOnPremiseTcpTunnelSilently();

        if (parseBootstrapAddresses(virtualBootstrapServers).isEmpty()) {
            throw new IllegalArgumentException(
                    "No valid virtual bootstrap host:port found for On-Premise mode: " + virtualBootstrapServers);
        }

        final String locationId = trimToNull(endpoint.getSapSapccLocationId());

        final SdiaCloudConnectorTcpTunnel.TunnelConfig tunnelConfig =
                SdiaCloudConnectorTcpProbe.resolveAndProbe(virtualBootstrapServers, locationId);

        final SdiaCloudConnectorTcpTunnel tunnel = new SdiaCloudConnectorTcpTunnel(tunnelConfig);
        tunnel.start();

        onPremiseTcpTunnel                = tunnel;
        onPremiseClientBootstrapServers   = tunnel.getLocalBootstrapServers();

        LOG.warn("[SDIA Kafka] SAP Cloud Connector TCP tunnel ready."
                + " virtualBootstrap=" + virtualBootstrapServers
                + " kafkaClientBootstrap=" + onPremiseClientBootstrapServers
                + " locationId=" + displayOptionalLocationId(locationId)
                + " cloudModeUnaffected=true");

        return onPremiseClientBootstrapServers;
    }

    private void closeOnPremiseTcpTunnelSilently() {
        final SdiaCloudConnectorTcpTunnel tunnel = onPremiseTcpTunnel;
        onPremiseTcpTunnel              = null;
        onPremiseClientBootstrapServers = null;
        if (tunnel != null) {
            try { tunnel.close(); } catch (Throwable ignored) {}
        }
    }

    // =========================================================================
    // Kafka consumer lifecycle helpers
    // =========================================================================

    private void closeKafkaConsumerSilently() {
        if (kafkaConsumer != null) {
            try { kafkaConsumer.wakeup(); }     catch (Throwable ignored) {}
            try { kafkaConsumer.close(Duration.ZERO); } catch (Throwable ignored) {}
            kafkaConsumer = null;
        }
    }

    /**
     * Resolves the ClassLoader that should be active during Kafka client
     * construction in an OSGi (SAP CPI) environment.
     *
     * <p>PlainLoginModule is used as the probe class because it is part of the
     * same bundle as the Kafka clients JAR and is always available when SASL/PLAIN
     * is used. For PLAINTEXT authentication the class is still loaded (its
     * ClassLoader is retrieved), but no login module is instantiated.
     */
    private ClassLoader resolveKafkaClassLoader() {
        final ClassLoader kafkaCl = PlainLoginModule.class.getClassLoader();
        return (kafkaCl != null) ? kafkaCl : SdiaKafkaConsumer.class.getClassLoader();
    }

    // =========================================================================
    // Normalisation / parsing helpers
    // =========================================================================

    private String normalizeErrorHandling(final String handling) {
        final String h = (handling == null) ? "Stop on Error" : handling.trim();
        if (h.isEmpty()) {
            return "Stop on Error";
        }
        final String l = h.toLowerCase();
        if (l.contains("skip"))  { return "Skip Failed Message"; }
        if (l.contains("retry")) { return "Retry Failed Message"; }
        if (l.contains("stop"))  { return "Stop on Error"; }
        return h;
    }

    private String normalizeAutoOffsetReset(final String configured) {
        final String value = (configured == null) ? "earliest" : configured.trim();
        if ("earliest".equalsIgnoreCase(value)) { return "earliest"; }
        if ("latest".equalsIgnoreCase(value))   { return "latest"; }
        if ("none".equalsIgnoreCase(value))     { return "none"; }
        LOG.warn("[SDIA Kafka] Invalid autoOffsetReset value [{}]. Falling back to earliest.", configured);
        return "earliest";
    }

    private String normalizeConversionFormat(final String format) {
        return (format == null) ? "None" : format.trim();
    }

    /**
     * Delegates authentication classification to {@link SdiaKafkaEndpoint}
     * so there is a single authoritative source. Returns true when the
     * authentication profile is NONE / PLAINTEXT (no SASL, credential not required).
     */
    private boolean isPlainTextAuthentication(final String authentication) {
        return !endpoint.isAuthenticationSaslEffective();
    }

    /**
     * Returns true only when the authentication profile is SASL.
     * Delegates to {@link SdiaKafkaEndpoint#isAuthenticationSaslEffective()}.
     */
    private boolean isSaslAuthentication(final String authentication) {
        return endpoint.isAuthenticationSaslEffective();
    }

    private boolean shouldUseMagicByte(final String mode, final String fixedId, final byte[] payload) {
        final String normalised = normalizeSchemaIdResolutionMode(mode);
        if ("MAGIC".equals(normalised))  { return true;  }
        if ("FIXED".equals(normalised))  { return false; }
        if (trimToNull(fixedId) != null) { return false; }
        return (payload != null && payload.length > 0 && payload[0] == 0x00);
    }

    private String normalizeSchemaIdResolutionMode(final String mode) {
        final String raw = trimToNull(mode);
        if (raw == null) { return ""; }
        final String m = raw.toLowerCase();
        if (m.contains("fixed") || m.contains("schemaid") || m.contains("schema id")) { return "FIXED"; }
        if (m.contains("magic") || m.contains("wire")     || m.contains("header"))    { return "MAGIC"; }
        return "";
    }

    private List<String> parseTopicExpression(final String expr) {
        if (expr == null) { return Collections.emptyList(); }
        final List<String> list = new ArrayList<>();
        for (final String s : expr.split(",")) {
            final String t = s.trim();
            if (!t.isEmpty()) { list.add(t); }
        }
        return list;
    }

    private boolean isPatternMode(final String token) {
        return token.contains("*") || token.contains("^") || token.contains("$");
    }

    private boolean containsPatternMode(final List<String> tokens) {
        for (final String t : tokens) {
            if (isPatternMode(t)) { return true; }
        }
        return false;
    }

    private Pattern compileTopicPatternExpression(final List<String> tokens) {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            final String regex = tokens.get(i).replace(".", "\\.").replace("*", ".*");
            sb.append('(').append(regex).append(')');
            if (i < tokens.size() - 1) { sb.append('|'); }
        }
        return Pattern.compile(sb.toString());
    }

    private List<String> findMissingTopics(final Map<String, List<PartitionInfo>> known,
                                            final List<String> targets) {
        final List<String> missing = new ArrayList<>();
        if (known == null) { return targets; }
        for (final String t : targets) {
            if (!known.containsKey(t)) { missing.add(t); }
        }
        return missing;
    }

    private List<TopicPartition> parsePartitions(final String topic, final String pParam) {
        final List<TopicPartition> list = new ArrayList<>();
        for (final String s : pParam.split(",")) {
            list.add(new TopicPartition(topic, Integer.parseInt(s.trim())));
        }
        return list;
    }

    // =========================================================================
    // Bootstrap address parsing (On-Premise)
    // =========================================================================

    private List<BootstrapAddress> parseBootstrapAddresses(final String bootstrapServers) {
        final List<BootstrapAddress> result = new ArrayList<>(4);
        final String value = trimToNull(bootstrapServers);
        if (value == null) { return result; }

        int start = 0;
        final int len = value.length();
        while (start < len) {
            int comma = value.indexOf(',', start);
            if (comma == -1) { comma = len; }
            final String entry = trimToNull(value.substring(start, comma));
            if (entry != null) {
                final BootstrapAddress parsed = parseBootstrapAddress(entry);
                if (parsed != null) { result.add(parsed); }
            }
            start = comma + 1;
        }
        return result;
    }

    private BootstrapAddress parseBootstrapAddress(final String entry) {
        final String value = trimToNull(entry);
        if (value == null) { return null; }
        final int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon >= value.length() - 1) {
            throw new IllegalArgumentException(
                    "Invalid Kafka bootstrap entry for On-Premise mode. Expected host:port, got: " + value);
        }
        final String host    = trimToNull(value.substring(0, colon));
        final String portStr = trimToNull(value.substring(colon + 1));
        if (host == null || portStr == null) {
            throw new IllegalArgumentException(
                    "Invalid Kafka bootstrap entry for On-Premise mode. Expected host:port, got: " + value);
        }
        try {
            final int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new NumberFormatException("port out of range");
            }
            return new BootstrapAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid Kafka bootstrap port for On-Premise mode. Entry: " + value, e);
        }
    }

    // =========================================================================
    // Error formatting helpers
    // =========================================================================

    private boolean isVisualAdapterError(final String value) {
        return value != null && value.contains("⛔ Event Smart Kafka Exception -");
    }

    private String buildVisualError(final String title, final String detail, final String correction) {
        return "\n\n-----\n"
                + "⛔ Event Smart Kafka Exception - " + title
                + "\n-----\n"
                + detail + "\n"
                + "\n-----\n"
                + "⚠️ Correction required:\n"
                + "\n-----\n"
                + correction + "\n";
    }

    private Exchange markError(final Exchange exchange, final byte[] rawBody,
                               final String errCode, final String format,
                               final String mode, final int schemaId, final String detail) {
        exchange.getIn().setBody(rawBody);
        exchange.getIn().setHeader("x-sdiakafka-error-code",         errCode);
        exchange.getIn().setHeader("x-sdiakafka-error-detail",       detail);
        exchange.getIn().setHeader("x-sdiakafka-resolved-schema-id", schemaId);
        exchange.getIn().setHeader("x-sdiakafka-schema-mode",        mode);
        exchange.setException(new RuntimeCamelException(detail));
        return exchange;
    }

    private String buildCompactAvroError(final ConsumerRecord<String, byte[]> record,
                                          final String errCode,
                                          final String schemaMode,
                                          final Object schemaId,
                                          final String detail,
                                          final boolean poisonOffsetCommitted) {
        final String cleanDetail = cleanAvroErrorDetail(detail);
        final String correction  = buildAvroCorrection(errCode, schemaMode, schemaId, cleanDetail);

        final StringBuilder err = new StringBuilder(768);
        err.append("\n-----\n");
        if (poisonOffsetCommitted) {
            err.append("⛔ Event Smart Kafka Exception - Avro Conversion Failure - poison offset committed; next offset will continue.");
        } else {
            err.append("⛔ Event Smart Kafka Exception - Avro Conversion Failure - offset not committed; retry scheduled.");
        }
        err.append("\n-----\n");
        err.append("➔ Record Topic: ").append(record.topic());
        err.append("\n➔ Target Offset: ").append(record.offset());
        err.append("\n➔ Target Partition: ").append(record.partition());
        err.append("\n➔ Host:Port: ").append(endpoint.resolveBootstrapServers());
        err.append("\n🔴 Error Code: ").append(errCode);
        err.append("\n🔴 Schema Mode: ").append(schemaMode);
        err.append("\n🔴 Schema ID: ").append(schemaId);
        err.append("\n🔴 Error detail: ").append(cleanDetail);
        if (correction != null && !correction.isEmpty()) {
            err.append("\n\n-----\n⚠️ Correction required:\n-----\n").append(correction);
        }
        return err.toString();
    }

    private String cleanAvroErrorDetail(final String detail) {
        if (detail == null) { return "Unknown exception root cause"; }
        final String d   = detail.trim();
        final String key = ", cause: org.apache.camel.RuntimeCamelException: ";
        final int idx    = d.indexOf(key);
        if (idx >= 0) {
            final String before = d.substring(0, idx).trim();
            final String after  = d.substring(idx + key.length()).trim();
            if (before.equals(after)) { return before; }
        }
        return d;
    }

    private String buildAvroCorrection(final String errCode, final String schemaMode,
                                        final Object schemaId, final String detail) {
        final String mode = (schemaMode == null) ? "" : schemaMode;
        final String d    = (detail == null) ? "" : detail.toLowerCase();

        if ("Event.Smart.Kafka.Invalid.SchemaID.Format".equalsIgnoreCase(errCode)
                || d.contains("fixed schema id must be") || d.contains("schemaregistryschemaid")) {
            return "\n1 - Fixed Schema ID must be a positive numeric Schema Registry ID."
                    + "\n2 - Do not use a subject name such as order.created-value or aaaa-value in the Schema ID field."
                    + "\n3 - Register the schema first, copy the numeric id returned by Schema Registry, then redeploy the iFlow."
                    + "\n4 - If the payload has a Confluent Magic Wire header, switch Schema ID Source to Magic Byte instead of Fixed Schema ID.";
        }
        if (d.contains("fixed schema id mismatch")) {
            return "\n1 - The Kafka record contains a Confluent wire header with a different Schema ID than the channel Fixed Schema ID."
                    + "\n2 - Either change the channel to Schema ID Source = Magic Byte, or produce the payload with the same Schema ID configured in the channel.";
        }
        if (d.contains("missing confluent avro magic byte")) {
            return "\n1 - The channel is reading the payload as Confluent Magic Wire, but the record does not start with 0x00."
                    + "\n2 - Produce a real Confluent binary payload: 0x00 + 4-byte Schema ID + Avro binary body."
                    + "\n3 - If the payload is raw Avro without the Confluent header, change Schema ID Source to Fixed Schema ID.";
        }
        if (d.contains("payload appears to be base64 text")) {
            return "\n1 - The Kafka record value is Base64 text, not Avro binary bytes."
                    + "\n2 - Decode the Base64 before producing to Kafka, or use a producer option that sends BINARY, not STRING.";
        }
        if (d.contains("invalid avro string length") || d.contains("payload ended unexpectedly")
                || d.contains("invalid avro bytes length")) {
            if ("FixedSchemaID".equalsIgnoreCase(mode)) {
                return "\n1 - The channel is forcing Fixed Schema ID " + schemaId + ", but the payload bytes do not match."
                        + "\n2 - If the payload has a Confluent header with another Schema ID, switch Schema ID Source to Magic Byte."
                        + "\n3 - Do not edit Avro binary/Base64 manually; one wrong length byte corrupts decoder alignment.";
            }
            return "\n1 - The payload is not valid Avro for the schema selected by the adapter."
                    + "\n2 - Confirm the payload Schema ID, schema field order, and whether the record is Confluent wire format or raw Avro binary.";
        }
        return "\n1 - Check whether the channel is configured for Magic Byte or Fixed Schema ID."
                + "\n2 - Check whether the Kafka record value is real binary Avro, not JSON text or Base64 text."
                + "\n3 - Check whether the schema used to produce the payload is the same schema used by the adapter to decode it.";
    }

    private String buildKafkaBrokerConnectionConfigurationError(final String cause,
                                                                  final String bootstrapServers) {
        final String cleanCause = (cause == null || cause.trim().isEmpty())
                ? "Kafka broker connection failed." : cause.trim();

        final StringBuilder detail = new StringBuilder(768);
        detail.append("➔ Configuration Field: kafkaClusterHostsTable / kafkaHostRow / kafkaPortRow");
        detail.append("\n➔ Bootstrap Servers: ").append(bootstrapServers == null ? "<empty>" : bootstrapServers);
        detail.append("\n➔ Topic: ").append(endpoint.getTopicPattern() == null ? "<empty>" : endpoint.getTopicPattern());
        detail.append("\n➔ Authentication: ").append(endpoint.getAuthentication() == null ? "<empty>" : endpoint.getAuthentication());
        if (isPlainTextAuthentication(endpoint.getAuthentication())) {
            detail.append("\n➔ Credential Alias: n/a — NONE | PLAINTEXT does not use User Credentials");
        } else {
            detail.append("\n➔ Credential Alias: ").append(endpoint.getCredentialAlias() == null ? "<empty>" : endpoint.getCredentialAlias());
        }
        detail.append("\n🔴 Error Code: Event.Smart.Kafka.Config.Broker.Connection.Failed");
        detail.append("\n🔴 Error detail: ").append(cleanCause);

        return buildVisualError(
                "Adapter Configuration Failure - Kafka Broker Connection Failed",
                detail.toString(),
                "1 - For NONE | PLAINTEXT, no Credential Alias is required and no password is loaded."
                        + "\n2 - Verify the Kafka bootstrap host/port and the broker advertised.listeners."
                        + "\n3 - If Proxy Type = On-Premise, verify the Cloud Connector virtual mapping and Location ID."
                        + "\n4 - Check firewall/Cloud Connector routing if the endpoint is private."
                        + "\n5 - Redeploy the iFlow after correcting the host/port. This startup error is non-retryable.");
    }

    private String buildAdapterConfigurationError(final IllegalArgumentException cfgEx) {
        final String raw   = (cfgEx == null || cfgEx.getMessage() == null)
                ? "Unknown adapter configuration error." : cfgEx.getMessage();
        final String clean = stripSdiaPrefix(raw);
        final String lower = clean.toLowerCase();

        if (lower.contains("schemaregistryschemaid") || lower.contains("fixed schema id")
                || lower.contains("schema id")) {
            final StringBuilder detail = new StringBuilder(512);
            detail.append("➔ Configuration Field: schemaRegistrySchemaId");
            detail.append("\n➔ Conversion Format: ").append(normalizeConversionFormat(endpoint.getEffectiveConversionFormat()));
            detail.append("\n➔ Schema ID Source: ").append(trimToNull(endpoint.getSchemaIdResolutionMode()) == null ? "<empty>" : endpoint.getSchemaIdResolutionMode());
            detail.append("\n➔ Host:Port: ").append(endpoint.resolveBootstrapServers());
            detail.append("\n🔴 Error Code: Event.Smart.Kafka.Config.Invalid.SchemaID");
            detail.append("\n🔴 Error detail: ").append(clean);
            return buildVisualError(
                    "Adapter Configuration Failure - Invalid Fixed Schema ID",
                    detail.toString(),
                    "1 - Fixed Schema ID must be a positive numeric Schema Registry ID, e.g. 1, 17, 100010."
                            + "\n2 - Do not put the subject name in this field."
                            + "\n3 - For Redpanda, register the schema first and copy the numeric id returned."
                            + "\n4 - If the payload is Confluent Wire format, use Schema ID Source = Magic Byte."
                            + "\n5 - Redeploy the iFlow after correcting the channel.");
        }

        return buildVisualError(
                "Adapter Configuration Failure",
                "➔ Host:Port: " + endpoint.resolveBootstrapServers()
                        + "\n➔ Topic: " + endpoint.getTopicPattern()
                        + "\n🔴 Error Code: Event.Smart.Kafka.Config.Invalid.Channel"
                        + "\n🔴 Error detail: " + clean,
                "1 - Review the adapter channel configuration."
                        + "\n2 - Correct the invalid field and redeploy the iFlow."
                        + "\n3 - This configuration error is non-retryable at runtime.");
    }

    private String stripSdiaPrefix(final String value) {
        if (value == null) { return ""; }
        String v = value.trim();
        if (v.startsWith("[SDIA Kafka]")) { v = v.substring("[SDIA Kafka]".length()).trim(); }
        return v;
    }

    private String formatOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets == null || offsets.isEmpty()) { return "{}"; }
        final StringBuilder sb = new StringBuilder(128);
        sb.append('{');
        boolean first = true;
        for (final Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            if (!first) { sb.append(", "); }
            first = false;
            final TopicPartition   tp = e.getKey();
            final OffsetAndMetadata om = e.getValue();
            sb.append(tp.topic()).append('-').append(tp.partition())
              .append("=>").append(om == null ? "null" : om.offset());
        }
        sb.append('}');
        return sb.toString();
    }

    private static void appendHexByte(final StringBuilder out, final byte value) {
        final int v = value & 0xFF;
        out.append(HEX[v >>> 4]).append(HEX[v & 0x0F]);
    }

    private static int safeSecondsToMillis(final Long seconds, final int defaultSeconds) {
        final long sec = (seconds == null || seconds.longValue() <= 0L) ? defaultSeconds : seconds.longValue();
        final long ms  = sec * 1_000L;
        return (ms > Integer.MAX_VALUE) ? Integer.MAX_VALUE : (int) ms;
    }

    private boolean isNoOffsetForPartitionFailure(final Throwable t) {
        Throwable cause = t;
        while (cause != null) {
            final String cls = cause.getClass().getName();
            final String msg = cause.getMessage();
            if (cls != null && cls.contains("NoOffsetForPartitionException")) { return true; }
            if (msg != null) {
                final String lower = msg.toLowerCase(java.util.Locale.ROOT);
                if (lower.contains("no offset") && lower.contains("partition")) { return true; }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private String rootCauseMessage(final Throwable t) {
        if (t == null) { return "Unknown Error Context"; }
        Throwable cause = t;
        while (cause.getCause() != null) { cause = cause.getCause(); }
        return cause.getMessage() != null ? cause.getMessage() : cause.getClass().getName();
    }

    private String trimToNull(final String str) {
        if (str == null) { return null; }
        final String t = str.trim();
        return t.isEmpty() ? null : t;
    }

    private String maskUrl(final String url) {
        return (url == null) ? "<empty>" : url;
    }

    private String displayOptionalLocationId(final String locationId) {
        final String value = trimToNull(locationId);
        if (value == null) { return "<not configured - optional/default Cloud Connector>"; }
        if (value.length() <= 2) { return "**"; }
        return value.charAt(0) + "***" + value.charAt(value.length() - 1);
    }

    private void validateConfiguration() {}

    private void registerRuntimeStatus(final String status,
                                        final ConsumerRecord<String, byte[]> r,
                                        final Throwable t) {}

    // =========================================================================
    // Static inner classes
    // =========================================================================

    private static final class BootstrapAddress {
        final String host;
        final int    port;

        BootstrapAddress(final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        String authority() {
            return host + ':' + port;
        }
    }
}
