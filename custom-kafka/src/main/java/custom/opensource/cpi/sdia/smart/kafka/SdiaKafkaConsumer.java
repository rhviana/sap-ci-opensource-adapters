/*
 * ============================================================================
 * Event Smart Kafka Adapter
 * SDIA — Semantic Domain Integration Architecture
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

import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.ScheduledPollConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sap.it.api.ITApiFactory;
import com.sap.it.api.msglog.adapter.AdapterMessageLogFactory;
import com.sap.it.api.msglog.adapter.AdapterMessageLogWithStatus;
import com.sap.it.api.msglog.adapter.AdapterStatusEvent;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

public class SdiaKafkaConsumer extends ScheduledPollConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(SdiaKafkaConsumer.class);
    private final SdiaKafkaEndpoint endpoint;
    private KafkaConsumer<String, byte[]> kafkaConsumer;
    private boolean assignMode = false;
    private String effectiveGroupId = "unknown";
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private boolean initialized = false;
    private volatile boolean recoverySeekApplied = false;
    private volatile long recoverySeekTimestampMs = -1L;
    private volatile String recoverySeekInput = null;
    private final Map<TopicPartition, Long> recoverySeekOffsets = new LinkedHashMap<TopicPartition, Long>();

    /**
     * Original committed offsets captured before an isolated single-message recovery seek.
     *
     * In "Read only first matched message" mode the adapter must NOT move the
     * official consumer-group position to the recovered offset. Otherwise, after
     * disabling Message Recovery, the channel resumes from the recovered offset + 1
     * and replays all later records.
     */
    private final Map<TopicPartition, OffsetAndMetadata> recoveryOriginalCommittedOffsets =
            new LinkedHashMap<TopicPartition, OffsetAndMetadata>();

    /**
     * Pending seek timestamp for "Read all from timestamp" mode.
     * Applied inside onPartitionsAssigned so the consumer stays in the group
     * and normal offset commits continue to work without rebalance failures.
     * -1L means no pending seek.
     */
    private volatile long pendingSubscribeSeekTimestampMs = -1L;

    /**
     * Message Recovery single-read guard.
     * When enabled, the adapter reads only the first record resolved by the recovery seek
     * and then remains silent until recovery is disabled/redeployed. This prevents a
     * controlled replay from continuing into the latest committed poison/exception offset.
     */
    private volatile boolean recoverySingleMessageCompleted = false;
    private volatile long recoveryCompletedOffset = -1L;
    private volatile String recoveryCompletedTopicPartition = null;

    /**
     * Avro/conversion failures are adapter-level failures, not Integration Flow processing failures.
     * They must NOT commit the failed offset. Deterministic poison payloads are parked terminally
     * for Stop/Retry policies to avoid reprocessing the same Kafka offset forever.
     */
    private volatile boolean fatalAvroConversionFailure = false;
    private volatile String fatalAvroConversionReason = null;
    private volatile long nextAvroRetryAllowedAtMs = 0L;
    private static final long AVRO_FAILURE_RETRY_BACKOFF_MS = 5000L;

    /**
     * Stop on Error is terminal for the current deployed consumer instance.
     * The failed offset is intentionally NOT committed. The scheduler remains alive,
     * but poll() returns without re-reading the same poison record until redeploy/restart.
     */
    private volatile boolean stoppedByErrorPolicy = false;
    private volatile String stoppedByErrorPolicyReason = null;
    private volatile long lastStoppedLogAtMs = 0L;

    /**
     * Fatal initialization failure — wrong config, unreachable broker, missing topic, etc.
     * Retry is allowed only for retryable runtime situations. Explicit channel/configuration
     * failures set nonRetryableConfigurationFailure and remain silent until redeploy.
     * Prevents ScheduledPollConsumer from flooding CPI with the same broker/port error.
     */
    private volatile boolean fatalInitFailure = false;
    private volatile String fatalInitReason = null;
    private volatile long nextInitRetryAllowedAtMs = 0L;

    /**
     * Non-retryable channel/configuration failures must not create an infinite
     * scheduled-poll loop. Example: Fixed Schema ID configured with "aaaa-value"
     * instead of a positive numeric Schema Registry ID. Only redeploy/start resets it.
     */
    private volatile boolean nonRetryableConfigurationFailure = false;

    private volatile long lastEmptyPollLogAtMs = 0L;

    /**
     * On-Premise / SAP Cloud Connector TCP bridge.
     *
     * Kafka's Java client does not understand SAP Cloud Connector virtual hosts directly.
     * In On-Premise mode the consumer starts a local HTTP CONNECT tunnel and rewrites
     * bootstrap.servers to 127.0.0.1:<same-port>. The tunnel forwards the raw Kafka TCP
     * stream through the SAP Cloud Connector proxy returned by CloudConnectorProperties.
     *
     * Required broker setup for this mode:
     *   - Cloud Connector virtual host maps to the internal Kafka broker host/port.
     *   - Kafka advertised.listeners for the external/client listener must advertise
     *     localhost:<port> or 127.0.0.1:<port>, because the CPI-side Kafka client will
     *     receive broker metadata and reconnect through the local tunnel on that port.
     */
    private volatile SdiaCloudConnectorTcpTunnel onPremiseTcpTunnel;
    private volatile String onPremiseClientBootstrapServers;

    /**
     * Diagnostic flag kept for backwards log compatibility.
     * Final On-Premise mode uses normal subscribe()/Kafka group management when
     * Partitions is empty. assign() is used only for explicit partitions or
     * deterministic Message Recovery, and commits are never bypassed.
     */
    private volatile boolean onPremiseForcedAssignMode = false;

    private static final long EMPTY_POLL_DIAG_LOG_EVERY_MS = 300000L;
    private static final Duration ADMIN_LIST_TOPICS_TIMEOUT = Duration.ofMillis(4000L);

    /**
     * Consumer fetch floor used only when Avro conversion is enabled.
     *
     * If users configure Max Payload Conversion Size = 1 MB and accidentally
     * produce a 2 MB Avro record, Kafka may otherwise keep returning empty polls
     * when max.partition.fetch.bytes is too small. The adapter must fetch the
     * record first, then reject it with a visible Payload.TooLarge / schema error.
     */
    private static final int AVRO_OVERSIZE_DETECTION_FETCH_FLOOR_BYTES = 5 * 1024 * 1024;
    private static final int AVRO_OVERSIZE_DETECTION_MARGIN_BYTES = 2 * 1024 * 1024;

    /**
     * Pre-allocated offset accumulator for the poll() batch.
     * Cleared at the start of each non-empty batch instead of being allocated anew.
     * Safe because KafkaConsumer is always accessed from a single Camel scheduler thread.
     * Initial capacity 4: most channels have 1-4 partitions.
     *
     * Store primitive next-offsets during the record loop. OffsetAndMetadata is allocated
     * only once per touched partition at commit time, not once per delivered record.
     */
    private final Map<TopicPartition, MutableOffset> pollSuccessOffsets = new LinkedHashMap<TopicPartition, MutableOffset>(4);
    private final Map<TopicPartition, OffsetAndMetadata> pollCommitOffsets = new LinkedHashMap<TopicPartition, OffsetAndMetadata>(4);

    /**
     * Cached values resolved once at initialization — avoids repeated string normalization
     * and virtual dispatch on every record in the poll() hot path.
     */
    private String cachedErrorHandling = "Stop on Error";
    private boolean cachedOnPremiseMode = false;
    private long    cachedPollTimeoutMs = 2000L;
    private Duration cachedPollDuration = Duration.ofMillis(2000L);

    /**
     * Adapter-configurable timeout for synchronous Kafka offset commits.
     * Kept as cached Duration so the commit hot path does not read endpoint metadata.
     */
    private Duration cachedOffsetCommitTimeout = Duration.ofSeconds(60L);

    private String  cachedConversionFormat = "None";
    private boolean cachedConversionRequested = false;
    private boolean cachedJsonSchemaPassthrough = false;
    private String  cachedSchemaRegistryUrl = null;
    private String  cachedRegistryCredentialAlias = null;
    private String  cachedFixedSchemaIdStr = null;
    private Integer cachedFixedSchemaId = null;
    private boolean cachedFixedSchemaIdInvalid = false;
    private String  cachedSchemaIdResolutionMode = null;
    private long    cachedMaxPayloadBytes = 10L * 1024L * 1024L;
    private int     cachedMaxPayloadSizeMb = 10;
    private int     cachedSchemaCacheMaxBytes = 250 * 1024;
    private int     cachedSchemaHeaderPreviewBytes = 16;
    private int     cachedSchemaRegistryConnectTimeoutMs = 5000;
    private int     cachedSchemaRegistryReadTimeoutMs = 10000;

    /**
     * TopicPartition objects are immutable and reused by Kafka commit maps.
     * Cache them per topic/partition to avoid one TopicPartition allocation per record.
     */
    private final Map<String, Map<Integer, TopicPartition>> topicPartitionCache =
            new HashMap<String, Map<Integer, TopicPartition>>(4);

    private static final class MutableOffset {
        long nextOffset;
    }

    public SdiaKafkaConsumer(final SdiaKafkaEndpoint endpoint, final Processor processor) {
        super(endpoint, processor);
        this.endpoint = endpoint;
        // ADK/CPI runtime contract: this adapter must start polling immediately after deployment.
        // Do not let old channel values for Initial Delay / Delay keep the route idle for minutes.
        this.setInitialDelay(0L);
        this.setDelay(1000L);
        this.setUseFixedDelay(true);
        try { this.setGreedy(true); } catch (Throwable ignored) {}
    }

    @Override
    protected void doStart() throws Exception {
        closed.set(false);
        this.kafkaConsumer = null;
        this.initialized = false;
        this.fatalAvroConversionFailure = false;
        this.fatalAvroConversionReason = null;
        this.recoverySingleMessageCompleted = false;
        this.recoveryCompletedOffset = -1L;
        this.recoveryCompletedTopicPartition = null;
        this.recoverySeekApplied = false;
        this.recoverySeekTimestampMs = -1L;
        this.recoverySeekInput = null;
        this.recoverySeekOffsets.clear();
        this.recoveryOriginalCommittedOffsets.clear();
        this.nextAvroRetryAllowedAtMs = 0L;
        this.stoppedByErrorPolicy = false;
        this.stoppedByErrorPolicyReason = null;
        this.fatalInitFailure = false;
        this.fatalInitReason = null;
        this.nextInitRetryAllowedAtMs = 0L;
        this.nonRetryableConfigurationFailure = false;
        this.lastEmptyPollLogAtMs = 0L;
        this.lastStoppedLogAtMs = 0L;
        this.pendingSubscribeSeekTimestampMs = -1L;
        this.onPremiseClientBootstrapServers = null;
        this.onPremiseForcedAssignMode = false;
        closeOnPremiseTcpTunnelSilently();

        // Do not clear the global Schema Registry cache on every iFlow start.
        // In a multi-iFlow CPI worker, one redeploy must not cold-flush schemas used by other live consumers.
        try { SdiaKafkaCredentialsResolver.clearCache(); } catch (Throwable ignored) {}

        // Force fast polling on every route start/redeploy. Hidden/persisted scheduling values
        // must not make the adapter look zombie after a new deployment.
        this.setInitialDelay(0L);
        this.setDelay(1000L);
        this.setUseFixedDelay(true);
        try { this.setGreedy(true); } catch (Throwable ignored) {}

        this.setExceptionHandler(new org.apache.camel.spi.ExceptionHandler() {
            @Override
            public void handleException(Throwable exception) {
                handleException("Kafka polling error", null, exception);
            }
            @Override
            public void handleException(String message, Throwable exception) {
                handleException(message, null, exception);
            }
            @Override
            public void handleException(String message, org.apache.camel.Exchange exchange, Throwable exception) {
                LOG.error("[SDIA Kafka] Polling exception caught by ExceptionHandler: " + exception.getMessage());
                try {
                    org.apache.camel.Exchange errorExchange =
                            (exchange != null) ? exchange : endpoint.createExchange();
                    errorExchange.setException(
                            exception instanceof RuntimeCamelException
                                    ? exception
                                    : new RuntimeCamelException(exception.getMessage(), exception));
                    getProcessor().process(errorExchange);
                } catch (Throwable t) {
                    LOG.error("[SDIA Kafka] ExceptionHandler delivery failed: " + t.getMessage());
                }
            }
        });

        LOG.info("[SDIA Kafka] doStart() complete — first poll will initialize the Kafka consumer.");

        try {
            String topic     = endpoint.getTopicPattern() != null ? endpoint.getTopicPattern() : "unknown-topic";
            String bootstrap = endpoint.resolveBootstrapServers() != null ? endpoint.resolveBootstrapServers() : "unknown-host";
            String iFlowId   = endpoint.getCamelContext() != null ? endpoint.getCamelContext().getName() : topic;
            SdiaKafkaEndpointInformationService.register(iFlowId, topic, bootstrap, "Starting...");
        } catch (Throwable ignored) {}

        super.doStart();
    }

    private void surfaceErrorToChannel(String errorMessage) {
        try {
            Exchange errorExchange = endpoint.createExchange();
            final String fullMsg = normalizeSurfaceError(errorMessage);
            final String cpiLastErrorMsg = toCpiLastErrorMessage(fullMsg);
            errorExchange.setException(new RuntimeCamelException(cpiLastErrorMsg));
            try {
                getProcessor().process(errorExchange);
            } catch (Throwable ignored) {
                getExceptionHandler().handleException("Adapter init failure", errorExchange,
                        new RuntimeCamelException(cpiLastErrorMsg));
            }
        } catch (Throwable t) {
            LOG.error("[SDIA Kafka] surfaceErrorToChannel failed: " + t.getMessage());
        }
    }

    private String normalizeSurfaceError(final String errorMessage) {
        if (isVisualAdapterError(errorMessage)) {
            // The underlying error (e.g. Topic.NotFound) is already a complete, well-formed
            // visual error block. Wrapping it again in an "Adapter Initialization Failure"
            // header here would duplicate the EventSmartKafka title and skip the blank-line
            // formatting that buildCompactRuntimeError applies, producing the doubled/concatenated
            // Last Error seen in CPI. Surface the inner error as-is instead.
            return extractBestVisualAdapterError(errorMessage);
        }
        return buildVisualError(
                "EventSmartKafka — Adapter Initialization Failure",
                "Bootstrap : " + safeValue(endpoint.resolveBootstrapServers())
                        + "\nTopic     : " + safeValue(endpoint.getTopicPattern())
                        + "\nProxy     : " + safeValue(endpoint.getProxyType())
                        + "\nSecurity  : " + describeSecurityProfile()
                        + "\nCredential: " + describeCredentialAlias()
                        + "\n\n" + safeRoot(errorMessage),
                "Review the adapter configuration and redeploy the iFlow.");
    }

    private String toCpiLastErrorMessage(final String message) {
        final String clean = trimLeadingLineBreaks(message == null ? "Unknown adapter failure." : message);
        // RuntimeCamelException is rendered by CPI as:
        //   org.apache.camel.RuntimeCamelException:<message>
        // Prefixing two line breaks forces a clean blank line between the class name and the visual error block.
        return "\n\n" + clean;
    }

    private String trimLeadingLineBreaks(final String value) {
        if (value == null) return "";
        int i = 0;
        while (i < value.length()) {
            final char c = value.charAt(i);
            if (c == '\n' || c == '\r') i++;
            else break;
        }
        return value.substring(i).trim();
    }

    private boolean isVisualAdapterError(final String value) {
        return nextVisualAdapterMarker(value, 0) >= 0;
    }

    private int nextVisualAdapterMarker(final String value, final int fromIndex) {
        if (value == null) return -1;
        int best = -1;
        // "⛔ SDIA - EventSmartKafka —" is the actual prefix produced by buildCompactRuntimeError()
        // (the function that formats every real adapter error, including Topic.NotFound,
        // Auth.Failed, etc). It must be checked first/explicitly: none of the older markers
        // below contain "SDIA - " as a substring, so a perfectly well-formed error was never
        // recognized as "already visual", causing normalizeSurfaceError() to wrap it a second
        // time in an "Adapter Initialization Failure" header and duplicate the title.
        best = minPositive(best, value.indexOf("⛔ SDIA - EventSmartKafka", fromIndex));
        best = minPositive(best, value.indexOf("⛔ EventSmartKafka", fromIndex));
        best = minPositive(best, value.indexOf("⛔ Event Smart Kafka", fromIndex));
        best = minPositive(best, value.indexOf("⛔ EventSmartKafka —", fromIndex));
        best = minPositive(best, value.indexOf("⛔ Event Smart Kafka Exception", fromIndex));
        return best;
    }

    private int minPositive(final int current, final int candidate) {
        if (candidate < 0) return current;
        if (current < 0) return candidate;
        return Math.min(current, candidate);
    }

    private String extractBestVisualAdapterError(final String value) {
        if (value == null || value.trim().isEmpty()) {
            return "⛔ SDIA - EventSmartKafka — Adapter failure\n\nCode      : Event.Smart.Kafka.Adapter.Failure"
                    + "\n\n🔎 Root cause:\n\nUnknown adapter failure."
                    + "\n\n🛠️ Fix:\n\n1. Review the adapter configuration and redeploy the iFlow.";
        }

        final String clean = trimLeadingLineBreaks(value);
        int selected = nextVisualAdapterMarker(clean, 0);
        if (selected < 0) return clean;

        int cursor = selected;
        while (cursor >= 0) {
            final String title = visualTitleAt(clean, cursor);
            if (!isAdapterInitializationWrapperTitle(title)) {
                selected = cursor;
                break;
            }
            final int next = nextVisualAdapterMarker(clean, cursor + 1);
            if (next < 0) break;
            cursor = next;
        }

        return trimLeadingLineBreaks(clean.substring(selected));
    }

    private String visualTitleAt(final String value, final int markerPos) {
        if (value == null || markerPos < 0 || markerPos >= value.length()) return "";
        int end = value.indexOf('\n', markerPos);
        if (end < 0) end = value.length();
        return value.substring(markerPos, end).trim();
    }

    private boolean isAdapterInitializationWrapperTitle(final String title) {
        if (title == null) return false;
        final String lower = title.toLowerCase(java.util.Locale.ROOT);
        return lower.indexOf("adapter initialization failure") >= 0;
    }

    private synchronized String tryDelayedInitialize() {
        if (initialized) return null;

        LOG.info("[SDIA Kafka] Initializing consumer. bootstrap=" + endpoint.resolveBootstrapServers()
                + " topic=" + endpoint.getTopicPattern());

        String result = doInitialize();
        if (result != null) {
            LOG.error("[SDIA Kafka] tryDelayedInitialize() FAILED: " + result);
        } else {
            LOG.warn("[SDIA Kafka] tryDelayedInitialize() SUCCESS");
        }
        return result;
    }

    private String doInitialize() {
        try {
            validateConfiguration();
            endpoint.validateConfiguration();
        } catch (IllegalArgumentException cfgEx) {
            this.nonRetryableConfigurationFailure = true;
            final String errorMsg = buildAdapterConfigurationError(cfgEx);
            LOG.error("[SDIA Kafka] " + errorMsg, cfgEx);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
            closeKafkaConsumerSilently();
            return errorMsg;
        }

        final String startupConversionFormat = normalizeConversionFormat(endpoint.getEffectiveConversionFormat());
        final boolean startupConversionRequested = !"None".equalsIgnoreCase(startupConversionFormat);

        if (startupConversionRequested) {
            String registryUrl = trimToNull(endpoint.getSchemaRegistryHostAddress());
            String registryAlias = trimToNull(endpoint.getSchemaRegistryCredentialAlias());
            String fixedSchemaIdStr = trimToNull(endpoint.getSchemaRegistrySchemaId());
            String schemaMode = trimToNull(endpoint.getSchemaIdResolutionMode());

            LOG.warn("[SDIA Kafka] [CONVERSION STARTUP BYPASS] Conversion=" + startupConversionFormat
                    + " | Mode=" + schemaMode
                    + " | Registry=" + maskUrl(registryUrl)
                    + " | CredentialAlias=" + (registryAlias == null ? "<empty>" : registryAlias)
                    + " | FixedSchemaId=" + (fixedSchemaIdStr == null ? "<empty>" : fixedSchemaIdStr)
                    + " | Runtime lookup happens on first record.");
        }

        String brokerAlias = trimToNull(endpoint.getCredentialAlias());
        if (isPlainTextAuthentication(endpoint.getAuthentication())) {
            if (brokerAlias != null) {
                LOG.warn("[SDIA Kafka] Authentication is NONE. Ignoring configured Credential Alias ["
                        + brokerAlias + "] for broker connection. No Secure Store/User Credential lookup will be performed.");
            } else {
                LOG.warn("[SDIA Kafka] Authentication is NONE. Broker credential is not required and will not be resolved.");
            }
        } else if (endpoint.isMtlsAuthentication(endpoint.getAuthentication())) {
            // mTLS — pre-validate that the keystore alias is resolvable.
            final String keyAlias = trimToNull(endpoint.getMtlsKeyAlias());
            if (keyAlias == null) {
                this.nonRetryableConfigurationFailure = true;
                String errorMsg = "[SDIA Kafka] mTLS authentication requires 'mtlsKeyAlias'. "
                        + "Provide a CPI Keystore alias containing the client private key and certificate chain.";
                LOG.error("[SDIA Kafka] " + errorMsg);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                return errorMsg;
            }
            LOG.warn("[SDIA Kafka] mTLS authentication selected. Client certificate will be loaded from Keystore alias: " + keyAlias);
        } else if (isSaslAuthentication(endpoint.getAuthentication())) {
            if (brokerAlias == null) {
                this.nonRetryableConfigurationFailure = true;
                String errorMsg = buildMissingBrokerCredentialError();
                LOG.error("[SDIA Kafka] " + errorMsg);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                return errorMsg;
            }
            try {
                SdiaKafkaCredentialsResolver.resolve(brokerAlias);
            } catch (Throwable credentialEx) {
                this.nonRetryableConfigurationFailure = true;
                String cause = rootCauseMessage(credentialEx);
                String errorMsg = buildBrokerCredentialResolutionError(brokerAlias, cause);
                LOG.error("[SDIA Kafka] " + errorMsg, credentialEx);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                return errorMsg;
            }
        }

        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader kafkaClassLoader = PlainLoginModule.class.getClassLoader() != null ? PlainLoginModule.class.getClassLoader() : SdiaKafkaConsumer.class.getClassLoader();

        KafkaConsumerFactory factory = new KafkaConsumerFactory();
        String bootstrapServers = endpoint.resolveBootstrapServers();
        if (bootstrapServers == null || bootstrapServers.trim().isEmpty()) {
            this.nonRetryableConfigurationFailure = true;
            String errorMsg = buildBootstrapServersMissingError();
            LOG.error("[SDIA Kafka] " + errorMsg);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
            return errorMsg;
        }

        String kafkaClientBootstrapServers = bootstrapServers;
        if (isOnPremiseProxyMode()) {
            try {
                kafkaClientBootstrapServers = ensureOnPremiseTcpTunnel(bootstrapServers);
                LOG.warn("[SDIA Kafka] On-Premise Cloud Connector tunnel active. "
                        + "virtualBootstrap=" + bootstrapServers
                        + " | localKafkaBootstrap=" + kafkaClientBootstrapServers
                        + " | locationId=" + displayOptionalLocationId(endpoint.getSapSapccLocationId()));
            } catch (Throwable tunnelEx) {
                final String root = rootCauseMessage(tunnelEx);
                this.nonRetryableConfigurationFailure = true;
                this.fatalInitFailure = true;
                this.fatalInitReason = root;
                String errorMsg = buildCloudConnectorTunnelError(bootstrapServers, root);
                LOG.error("[SDIA Kafka] " + errorMsg, tunnelEx);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeOnPremiseTcpTunnelSilently();
                return errorMsg;
            }
        }

        LOG.warn("[SDIA Kafka] Bootstrap servers resolved: " + bootstrapServers
                + " | kafkaClientBootstrapServers=" + kafkaClientBootstrapServers);

        try {
            Thread.currentThread().setContextClassLoader(kafkaClassLoader);
            Properties props = factory.createProperties(endpoint);
            applyOnPremiseClientBootstrapIfRequired(props, kafkaClientBootstrapServers);

            applyConsumerRuntimeProperties(props, false);
            LOG.warn("[SDIA Kafka] Kafka security profile resolved."
                    + " | authentication=" + endpoint.getAuthentication()
                    + " | connectWithTls=" + endpoint.getConnectWithTls()
                    + " | security.protocol=" + String.valueOf(props.get("security.protocol"))
                    + " | sasl.mechanism=" + String.valueOf(props.get("sasl.mechanism"))
                    + " | credentialAlias=" + (trimToNull(endpoint.getCredentialAlias()) == null ? "<empty>" : endpoint.getCredentialAlias())
                    + " | onPremise=" + isOnPremiseProxyMode());

            Map<String, List<PartitionInfo>> knownTopics;
            try {
                Properties adminProps = factory.createProperties(endpoint);
                applyOnPremiseClientBootstrapIfRequired(adminProps, kafkaClientBootstrapServers);
                adminProps.remove(ConsumerConfig.GROUP_ID_CONFIG);
                applyConsumerRuntimeProperties(adminProps, true);
                KafkaConsumer<String, byte[]> adminConsumer = new KafkaConsumer<>(adminProps);
                try {
                    knownTopics = adminConsumer.listTopics(ADMIN_LIST_TOPICS_TIMEOUT);
                } finally {
                    try { adminConsumer.close(Duration.ZERO); } catch (Throwable ignored) {}
                }
            } catch (Throwable netEx) {
                String cause = rootCauseMessage(netEx);
                this.nonRetryableConfigurationFailure = true;
                this.fatalInitFailure = true;
                this.fatalInitReason = cause;
                String errorMsg = buildKafkaBrokerConnectionConfigurationError(cause, endpoint.resolveBootstrapServers());
                LOG.error("[SDIA Kafka] " + errorMsg, netEx);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return errorMsg;
            }

            this.kafkaConsumer = new KafkaConsumer<>(props);

            String partitionsParam = trimToNull(endpoint.getPartitions());
            String topicExpression = trimToNull(endpoint.getTopicPattern());
            List<String> topicEntries = parseTopicExpression(topicExpression);

            // ── Parallel consumers + explicit partition conflict ───────────────
            // Explicit partition uses assign() which bypasses the consumer group.
            // Parallelism requires subscribe() + group rebalance to distribute
            // partitions. The two modes are mutually exclusive.
            final int parallelCount = endpoint.getParallelConsumers() != null
                    ? endpoint.getParallelConsumers().intValue() : 1;
            if (parallelCount > 1 && partitionsParam != null) {
                this.nonRetryableConfigurationFailure = true;
                String errorMsg = buildPartitionConfigurationError(topicExpression,
                        "Parallel Consumers > 1 cannot be combined with explicit Partition configuration. "
                        + "Remove the Partition field to allow Kafka to distribute partitions automatically "
                        + "among the " + parallelCount + " parallel consumers via group rebalance.");
                LOG.error("[SDIA Kafka] " + errorMsg);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return errorMsg;
            }
            this.effectiveGroupId = String.valueOf(props.get(ConsumerConfig.GROUP_ID_CONFIG));
            if (this.effectiveGroupId == null
                    || this.effectiveGroupId.trim().isEmpty()
                    || "null".equalsIgnoreCase(this.effectiveGroupId.trim())) {
                this.nonRetryableConfigurationFailure = true;
                String errorMsg = buildConsumerGroupMissingError();
                LOG.error("[SDIA Kafka] " + errorMsg);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return errorMsg;
            }

            LOG.warn("[SDIA Kafka] Consumer runtime initialized with group.id=[" + this.effectiveGroupId
                    + "] | assignMode=" + (partitionsParam != null)
                    + " | onPremise=" + isOnPremiseProxyMode()
                    + " | onPremiseForcedAssignMode=" + onPremiseForcedAssignMode
                    + " | topicExpression=[" + topicExpression + "]");

            this.onPremiseForcedAssignMode = false;
            if (partitionsParam != null) {
                if (topicEntries.size() != 1 || isPatternMode(topicEntries.get(0))) {
                    this.nonRetryableConfigurationFailure = true;
                    String errorMsg = buildPartitionConfigurationError(topicExpression, "Explicit partitions can only be used with one single exact topic.");
                    LOG.error("[SDIA Kafka] " + errorMsg);
                    try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                    closeKafkaConsumerSilently();
                    return errorMsg;
                }

                String exactTopic = topicEntries.get(0);
                assignMode = true;
                List<TopicPartition> topicPartitions = parsePartitions(exactTopic, partitionsParam);
                kafkaConsumer.assign(topicPartitions);
                applyAssignedStartOffsets(topicPartitions,
                        (isOnPremiseProxyMode() ? "on-premise" : "cloud") + " explicit channel partitions");
            } else {
                assignMode = false;

                // "Read all from timestamp" — consumer stays in subscribe() group.
                // Seek is registered here and applied inside onPartitionsAssigned
                // so normal offset commits continue to work (no unsubscribe/assign needed).
                final boolean readAllRecovery = endpoint.isMessageRecoveryEnabled()
                        && !endpoint.isRecoverySingleMessageOnly()
                        && endpoint.getSeekToTimestampMs() > 0L;

                if (readAllRecovery) {
                    this.pendingSubscribeSeekTimestampMs = endpoint.getSeekToTimestampMs();
                    this.recoverySeekInput = endpoint.getRecoveryTimestampValue();
                    this.recoverySeekTimestampMs = endpoint.getSeekToTimestampMs();
                    LOG.warn("[SDIA Kafka] Read-all recovery: seek will be applied via onPartitionsAssigned."
                            + " | timestampMs=" + endpoint.getSeekToTimestampMs()
                            + " | group.id=" + effectiveGroupId
                            + " | normalGroupCommitsActive=true");
                } else {
                    this.pendingSubscribeSeekTimestampMs = -1L;
                }

                org.apache.kafka.clients.consumer.ConsumerRebalanceListener rebalanceListener =
                        new org.apache.kafka.clients.consumer.ConsumerRebalanceListener() {
                            @Override
                            public void onPartitionsRevoked(
                                    java.util.Collection<TopicPartition> partitions) {}

                            @Override
                            public void onPartitionsAssigned(
                                    java.util.Collection<TopicPartition> partitions) {
                                final long seekTs = pendingSubscribeSeekTimestampMs;
                                if (seekTs <= 0L || partitions == null || partitions.isEmpty()) {
                                    return;
                                }
                                try {
                                    final Map<TopicPartition, Long> timestamps =
                                            new LinkedHashMap<TopicPartition, Long>();
                                    for (TopicPartition tp : partitions) {
                                        timestamps.put(tp, Long.valueOf(seekTs));
                                    }
                                    final Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsets =
                                            kafkaConsumer.offsetsForTimes(timestamps);

                                    int applied = 0;
                                    for (TopicPartition tp : partitions) {
                                        final org.apache.kafka.clients.consumer.OffsetAndTimestamp oat =
                                                offsets != null ? offsets.get(tp) : null;
                                        if (oat != null) {
                                            kafkaConsumer.seek(tp, oat.offset());
                                            recoverySeekOffsets.put(tp, Long.valueOf(oat.offset()));
                                            applied++;
                                            LOG.warn("[SDIA Kafka] Read-all recovery seek applied."
                                                    + " | topic=" + tp.topic()
                                                    + " | partition=" + tp.partition()
                                                    + " | timestampMs=" + seekTs
                                                    + " | offset=" + oat.offset());
                                        } else {
                                            kafkaConsumer.seekToEnd(
                                                    java.util.Collections.singletonList(tp));
                                            LOG.warn("[SDIA Kafka] Read-all recovery: no offset for timestamp,"
                                                    + " seekToEnd applied."
                                                    + " | topic=" + tp.topic()
                                                    + " | partition=" + tp.partition());
                                        }
                                    }
                                    pendingSubscribeSeekTimestampMs = -1L;
                                    recoverySeekApplied = true;
                                    LOG.warn("[SDIA Kafka] Read-all recovery seek completed via onPartitionsAssigned."
                                            + " | group.id=" + effectiveGroupId
                                            + " | timestampMs=" + seekTs
                                            + " | partitions=" + partitions.size()
                                            + " | appliedSeeks=" + applied
                                            + " | normalGroupCommitsActive=true");
                                } catch (Throwable seekEx) {
                                    LOG.error("[SDIA Kafka] Read-all recovery seek failed in onPartitionsAssigned: "
                                            + rootCauseMessage(seekEx), seekEx);
                                }
                            }
                        };

                if (containsPatternMode(topicEntries)) {
                    Pattern pattern = compileTopicPatternExpression(topicEntries);
                    kafkaConsumer.subscribe(pattern, rebalanceListener);
                    LOG.warn("[SDIA Kafka] Consumer subscribed by topic pattern. Normal Kafka group management is active."
                            + " | onPremise=" + isOnPremiseProxyMode()
                            + " | group.id=" + effectiveGroupId
                            + " | topicPattern=" + topicExpression
                            + " | readAllRecovery=" + readAllRecovery);
                } else {
                    List<String> missingTopics = findMissingTopics(knownTopics, topicEntries);
                    if (!missingTopics.isEmpty()) {
                        this.nonRetryableConfigurationFailure = true;
                        String errorMsg = buildMissingTopicError(missingTopics);
                        LOG.error("[SDIA Kafka] " + errorMsg);
                        try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
                        closeKafkaConsumerSilently();
                        return errorMsg;
                    }
                    kafkaConsumer.subscribe(topicEntries, rebalanceListener);
                    LOG.warn("[SDIA Kafka] Consumer subscribed by exact topic list. Normal Kafka group management is active."
                            + " | onPremise=" + isOnPremiseProxyMode()
                            + " | group.id=" + effectiveGroupId
                            + " | topics=" + topicEntries
                            + " | readAllRecovery=" + readAllRecovery);

                    // Warn if parallelConsumers > total partition count
                    if (parallelCount > 1 && knownTopics != null) {
                        int totalPartitions = 0;
                        for (String t : topicEntries) {
                            java.util.List<org.apache.kafka.common.PartitionInfo> pInfo = knownTopics.get(t);
                            if (pInfo != null) totalPartitions += pInfo.size();
                        }
                        if (totalPartitions > 0 && parallelCount > totalPartitions) {
                            LOG.warn("[SDIA Kafka] ⚠️ PARALLEL CONSUMERS WARNING: parallelConsumers=" + parallelCount
                                    + " exceeds total topic partitions=" + totalPartitions
                                    + ". Only " + totalPartitions + " consumer(s) will be active."
                                    + " The remaining " + (parallelCount - totalPartitions) + " will be idle."
                                    + " Set parallelConsumers <= " + totalPartitions + " to avoid idle threads."
                                    + " | topics=" + topicEntries);
                        }
                    }
                }
            }

            String recoveryError = applyMessageRecoverySeekIfConfigured(topicEntries, knownTopics, partitionsParam);
            if (recoveryError != null) {
                this.nonRetryableConfigurationFailure = true;
                LOG.error("[SDIA Kafka] " + recoveryError);
                try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), recoveryError); } catch (Throwable t) {}
                closeKafkaConsumerSilently();
                return recoveryError;
            }

            String currentTopic = topicExpression != null ? topicExpression : "unknown";
            String kafkaBrokerHost = endpoint.resolveBootstrapServers() != null ? endpoint.resolveBootstrapServers() : "unknown-host";
            String iFlowId = endpoint.getCamelContext() != null ? endpoint.getCamelContext().getName() : currentTopic;
            SdiaKafkaEndpointInformationService.register(iFlowId, currentTopic, kafkaBrokerHost, "Successful");

            // Cache hot-path values after successful initialization.
            refreshCachedHotPathConfig();

            this.initialized = true;
            return null;

        } catch (Exception e) {
            final String root = rootCauseMessage(e);
            this.nonRetryableConfigurationFailure = true;
            this.fatalInitFailure = true;
            this.fatalInitReason = root;
            String errorMsg = buildUnexpectedStartupError(root);
            LOG.error("[SDIA Kafka] " + errorMsg, e);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), errorMsg); } catch (Throwable t) {}
            closeKafkaConsumerSilently();
            return errorMsg;
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    @Override
    protected int poll() throws Exception {
        if (closed.get()) return 0;

        if (stoppedByErrorPolicy) {
            // Stop on Error is terminal for this deployed adapter instance.
            // Do not heartbeat-poll here: keeping a paused consumer alive can hold the
            // partition assignment after CPI redeploy/channel changes and create the
            // 5-10 minute "zombie" symptom. The KafkaConsumer is closed when STOP is
            // activated, so the group is released quickly and this instance remains silent.
            final long nowMs = System.currentTimeMillis();
            if (nowMs - lastStoppedLogAtMs > 60000L) {
                lastStoppedLogAtMs = nowMs;
                LOG.warn("[SDIA Kafka] Consumer is STOPPED by error policy. "
                        + "Undeploy and redeploy the iFlow to resume consumption. "
                        + "reason=" + (stoppedByErrorPolicyReason == null ? "<unknown>" : stoppedByErrorPolicyReason));
            }
            return 0;
        }

        if (recoverySingleMessageCompleted) {
            // Message Recovery in single-record mode is intentionally terminal for this
            // deployed instance. The recovered record was already processed/attempted.
            // Do not continue into the next Kafka offset. Disable Message Recovery and
            // redeploy to resume normal consumption.
            return 0;
        }

        if (fatalAvroConversionFailure) {
            long now = System.currentTimeMillis();
            if (now < nextAvroRetryAllowedAtMs) {
                return 0;
            }
            this.fatalAvroConversionFailure = false;
        }

        if (fatalInitFailure) {
            // Any failure — broker offline, cert wrong, config error, SOCKS5 down — stops here.
            // No retry. Operator must fix the issue and redeploy.
            return 0;
        }

        if (!initialized) {
            String initError = tryDelayedInitialize();
            if (initError != null) {
                this.fatalInitFailure = true;
                this.nonRetryableConfigurationFailure = true;
                this.fatalInitReason = initError;
                surfaceErrorToChannel(initError);
                return 0;
            }
        }

        if (kafkaConsumer == null) {
            String err = "kafkaConsumer is null after initialization. Internal runtime state error — redeploy required.";
            this.fatalInitFailure = true;
            this.nonRetryableConfigurationFailure = true;
            this.fatalInitReason = err;
            this.initialized = false;
            surfaceErrorToChannel(err);
            return 0;
        }

        ConsumerRecords<String, byte[]> records;
        try {
            // Use cached timeout — avoids endpoint getter + branch on every poll()
            if (LOG.isDebugEnabled()) {
                LOG.debug("[SDIA Kafka] poll group.id=" + effectiveGroupId + " timeoutMs=" + cachedPollTimeoutMs);
            }
            records = kafkaConsumer.poll(cachedPollDuration);
        } catch (WakeupException we) {
            if (closed.get()) return 0;
            throw we;
        } catch (Throwable pollEx) {
            String err;
            if (isNoOffsetForPartitionFailure(pollEx)) {
                err = buildNoCommittedOffsetError(rootCauseMessage(pollEx));
                this.nonRetryableConfigurationFailure = true;
            } else {
                err = buildPollFailureError(rootCauseMessage(pollEx));
                // If we are in On-Premise mode and the poll fails, the Cloud Connector
                // tunnel has dropped during active consumption. This is treated as
                // nonRetryable for the same reason as tunnel startup failures:
                // the operator must verify CC is up and redeploy the iFlow.
                // Retrying silently would re-initialize the tunnel and start consuming
                // from a potentially stale offset without the operator being aware.
                if (isOnPremiseProxyMode()) {
                    this.nonRetryableConfigurationFailure = true;
                    closeOnPremiseTcpTunnelSilently();
                }
            }
            registerRuntimeStatus("Event.Smart.Kafka.Engine.Poll.Failure", null, pollEx);
            LOG.error("[SDIA Kafka] " + err, pollEx);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), err); } catch (Throwable ignored) {}

            closeKafkaConsumerSilently();
            this.initialized = false;
            this.fatalInitFailure = true;
            this.nonRetryableConfigurationFailure = true;
            this.fatalInitReason = err;
            try { surfaceErrorToChannel(err); } catch (Throwable ignored) {}
            return 0;
        }

        if (records == null || records.isEmpty()) {
            logEmptyPollHeartbeat();
            return 0;
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("[SDIA Kafka] poll batch count=" + records.count());
        }
        int delivered = 0;
        // Reuse pre-allocated map — cleared here, not in every loop iteration
        pollSuccessOffsets.clear();
        final Map<TopicPartition, MutableOffset> successOffsets = pollSuccessOffsets;

        for (ConsumerRecord<String, byte[]> record : records) {
            if (closed.get() || fatalAvroConversionFailure) break;

            Exchange exchange = null;
            try {
                exchange = processRecord(record);

                if (exchange.getException() != null) {
                    Throwable conversionEx = exchange.getException();
                    String errCode = String.valueOf(exchange.getIn().getHeader("x-sdiakafka-error-code", String.class));
                    String headerDetail = exchange.getIn().getHeader("x-sdiakafka-error-detail", String.class);
                    String originalMessage = (headerDetail != null && headerDetail.trim().length() > 0)
                            ? headerDetail
                            : (conversionEx.getMessage() != null ? conversionEx.getMessage() : "Unknown exception root cause");
                    String schemaMode = String.valueOf(exchange.getIn().getHeader("x-sdiakafka-schema-mode", String.class));
                    Object schemaId = exchange.getIn().getHeader("x-sdiakafka-resolved-schema-id");

                    // Avro conversion errors are deterministic: the same bytes will not magically
                    // become valid Avro in the next poll. Therefore Skip Failed Message must behave
                    // as a poison-pill skip for conversion failures:
                    // 1) surface a real RuntimeCamelException to CPI/Camel,
                    // 2) commit failed offset + 1 immediately,
                    // 3) recycle the consumer so the next poll starts from the next committed offset.
                    // This gives visible error + no infinite loop.
                    String handling = cachedErrorHandling;
                    boolean skipPoisonCommit = "Skip Failed Message".equalsIgnoreCase(handling);
                    String fatalMsg = buildCompactAvroError(record, errCode, schemaMode, schemaId, originalMessage, skipPoisonCommit);

                    if (skipPoisonCommit) {
                        commitAvroPoisonOffsetThenRecycle(record, fatalMsg, conversionEx);
                        if (shouldStopAfterRecoveredRecord(record)) {
                            markRecoverySingleMessageCompleted(record, "skipped after conversion failure");
                            return delivered + 1;
                        }
                        throw new RuntimeCamelException(fatalMsg);
                    }

                    // Stop on Error / Retry: strict behavior. Do not commit the failed offset.
                    registerRuntimeStatus("Event.Smart.Kafka.Avro.FatalStop", record, conversionEx);
                    LOG.error("[SDIA Kafka] " + oneLineForLog(fatalMsg) + " | rootCause=" + safeLine(rootCauseMessage(conversionEx), 180));

                    commitSuccessOffsets(successOffsets, delivered);

                    if ("Stop on Error".equalsIgnoreCase(handling)) {
                        // STOP must be visible but terminal:
                        // 1) publish one failed exchange to the iFlow/Message Monitor with the failed offset headers,
                        // 2) do NOT commit the failed offset,
                        // 3) close/park this consumer instance,
                        // 4) do NOT throw to ScheduledPollConsumer, avoiding route restart and next-offset consumption.
                        publishStopErrorExchangeToIFlow(exchange, record, fatalMsg, conversionEx, "Avro conversion failure");
                        haltConsumerAfterFatalAvroFailure(record, fatalMsg, conversionEx);
                        return delivered;
                    }

                    haltConsumerAfterFatalAvroFailure(record, fatalMsg, conversionEx);
                    throw new RuntimeCamelException(fatalMsg);
                }

                processPipelineExchangeWithPolicy(exchange);

                markSuccessOffset(successOffsets, record);
                delivered++;

                if (shouldStopAfterRecoveredRecord(record)) {
                    // Single-message recovery is an isolated replay. Do NOT commit
                    // record.offset()+1 as the official consumer-group position;
                    // otherwise disabling recovery resumes from the recovered offset.
                    restoreOriginalCommittedOffsetsAfterSingleRecovery(record, "success");
                    markRecoverySingleMessageCompleted(record, "success");
                    return delivered;
                }

            } catch (Throwable ex) {
                if (isAdapterGeneratedAvroException(ex)) {
                    throw (RuntimeCamelException) ex;
                }

                String errorDetail = ex.getMessage() != null ? ex.getMessage() : ex.getClass().getName();
                String processorFatalMsg = buildPipelineProcessorError(record, errorDetail);

                if (exchange != null) {
                    exchange.setException(new RuntimeCamelException(processorFatalMsg));
                }

                registerRuntimeStatus("Event.Smart.Kafka.Record.Processing.Failure", record, ex);
                LOG.error("[SDIA Kafka] " + processorFatalMsg, ex);

                String handling = cachedErrorHandling;

                if ("Skip Failed Message".equalsIgnoreCase(handling)) {
                    markSuccessOffset(successOffsets, record);
                    delivered++;

                    if (shouldStopAfterRecoveredRecord(record)) {
                        // SKIP in isolated recovery must not move the official
                        // consumer-group offset to the recovered record + 1.
                        restoreOriginalCommittedOffsetsAfterSingleRecovery(record, "skipped after pipeline failure");
                        markRecoverySingleMessageCompleted(record, "skipped after pipeline failure");
                        return delivered;
                    }

                    continue;
                }

                commitSuccessOffsets(successOffsets, delivered);
                haltConsumerAfterPipelineFailure(record, processorFatalMsg, ex);

                if ("Stop on Error".equalsIgnoreCase(handling)
                        || "Retry Failed Message".equalsIgnoreCase(handling)) {
                    // Same rule as conversion Stop/Retry terminal failures: do not bubble the exception into
                    // ScheduledPollConsumer/Camel, because that may restart the route and re-read the same offset.
                    return delivered;
                }

                if (ex instanceof Exception) {
                    throw (Exception) ex;
                } else {
                    throw new RuntimeCamelException(ex);
                }
            }
        }

        commitSuccessOffsets(successOffsets, delivered);
        return delivered;
    }


    private void refreshCachedHotPathConfig() {
        this.cachedErrorHandling = normalizeErrorHandling(endpoint.getErrorHandling());
        this.cachedOnPremiseMode = isOnPremiseProxyMode();

        long pt = endpoint.getPollTimeoutMs();
        if (pt <= 0L) pt = 2000L;
        if (this.cachedOnPremiseMode && pt > 2000L) pt = 2000L;
        this.cachedPollTimeoutMs = pt;
        this.cachedPollDuration = Duration.ofMillis(pt);

        Long commitTimeoutMs = endpoint.getOffsetCommitTimeoutMs();
        long ctm = commitTimeoutMs == null ? 60000L : commitTimeoutMs.longValue();
        if (ctm < 5000L) ctm = 5000L;
        if (ctm > 300000L) ctm = 300000L;
        this.cachedOffsetCommitTimeout = Duration.ofMillis(ctm);

        final String format = normalizeConversionFormat(endpoint.getEffectiveConversionFormat());
        this.cachedConversionFormat = format;
        this.cachedJsonSchemaPassthrough = "JSON_SCHEMA".equalsIgnoreCase(format);
        this.cachedConversionRequested = !"None".equalsIgnoreCase(format) && !this.cachedJsonSchemaPassthrough;
        this.cachedSchemaRegistryUrl = trimToNull(endpoint.getSchemaRegistryHostAddress());
        this.cachedRegistryCredentialAlias = trimToNull(endpoint.getSchemaRegistryCredentialAlias());
        this.cachedFixedSchemaIdStr = trimToNull(endpoint.getSchemaRegistrySchemaId());
        this.cachedSchemaIdResolutionMode = endpoint.getSchemaIdResolutionMode();
        this.cachedFixedSchemaId = null;
        this.cachedFixedSchemaIdInvalid = false;
        if (this.cachedFixedSchemaIdStr != null) {
            try {
                final Integer parsed = Integer.valueOf(this.cachedFixedSchemaIdStr);
                if (parsed.intValue() <= 0) {
                    this.cachedFixedSchemaIdInvalid = true;
                } else {
                    this.cachedFixedSchemaId = parsed;
                }
            } catch (NumberFormatException nfe) {
                this.cachedFixedSchemaIdInvalid = true;
            }
        }
        this.cachedMaxPayloadBytes = endpoint.getMaxPayloadSizeBytes();
        final Integer mb = endpoint.getMaxPayloadSizeMb();
        this.cachedMaxPayloadSizeMb = mb == null ? 10 : mb.intValue();
        this.cachedSchemaCacheMaxBytes = endpoint.getSchemaCacheMaxBytes();
        this.cachedSchemaHeaderPreviewBytes = endpoint.getEffectiveSchemaIdSizeBuffer();
        this.cachedSchemaRegistryConnectTimeoutMs = endpoint.getSchemaRegistryConnectTimeoutMs().intValue();
        this.cachedSchemaRegistryReadTimeoutMs = endpoint.getSchemaRegistryReadTimeoutMs().intValue();
        this.topicPartitionCache.clear();
    }

    private void markSuccessOffset(final Map<TopicPartition, MutableOffset> successOffsets,
                                   final ConsumerRecord<String, byte[]> record) {
        final TopicPartition tp = topicPartition(record);
        MutableOffset offset = successOffsets.get(tp);
        if (offset == null) {
            offset = new MutableOffset();
            successOffsets.put(tp, offset);
        }
        offset.nextOffset = record.offset() + 1L;
    }

    private TopicPartition topicPartition(final ConsumerRecord<String, byte[]> record) {
        return topicPartition(record.topic(), record.partition());
    }

    private TopicPartition topicPartition(final String topic, final int partition) {
        Map<Integer, TopicPartition> byPartition = topicPartitionCache.get(topic);
        if (byPartition == null) {
            byPartition = new HashMap<Integer, TopicPartition>(4);
            topicPartitionCache.put(topic, byPartition);
        }
        final Integer key = Integer.valueOf(partition);
        TopicPartition tp = byPartition.get(key);
        if (tp == null) {
            tp = new TopicPartition(topic, partition);
            byPartition.put(key, tp);
        }
        return tp;
    }

    private boolean shouldStopAfterRecoveredRecord(final ConsumerRecord<String, byte[]> record) {
        if (record == null || !endpoint.isMessageRecoveryEnabled() || !endpoint.isRecoverySingleMessageOnly()) {
            return false;
        }

        final TopicPartition tp = topicPartition(record);
        final Long seekOffset = recoverySeekOffsets.get(tp);

        // Only stop after the record that was selected by offsetsForTimes().
        // Do not stop on unrelated records if recovery is disabled or if the seek was not applied.
        return recoverySeekApplied && seekOffset != null && record.offset() == seekOffset.longValue();
    }

    private void markRecoverySingleMessageCompleted(final ConsumerRecord<String, byte[]> record, final String result) {
        this.recoverySingleMessageCompleted = true;
        if (record != null) {
            this.recoveryCompletedOffset = record.offset();
            this.recoveryCompletedTopicPartition = record.topic() + "-" + record.partition();
        }

        LOG.warn("[SDIA Kafka] Message Recovery single-record mode completed. "
                + "The adapter will not consume the next offset until Message Recovery is disabled and the iFlow is redeployed."
                + " | result=" + (result == null ? "<unknown>" : result)
                + " | topicPartition=" + (recoveryCompletedTopicPartition == null ? "<unknown>" : recoveryCompletedTopicPartition)
                + " | recoveredOffset=" + recoveryCompletedOffset
                + " | recoveryInput=" + recoverySeekInput
                + " | recoveryTimestampMs=" + recoverySeekTimestampMs
                + " | officialGroupOffsetPreserved=true");
    }

    /**
     * Restores the committed offsets that existed before an isolated single-message recovery.
     *
     * Why this exists:
     * - Kafka seek(timestamp) moves the live consumer position.
     * - A normal success/SKIP commit would store recoveredOffset+1 as the official group offset.
     * - After disabling Message Recovery, the channel would then replay every offset after the
     *   recovered message.
     *
     * In "Read only first matched message" mode, recovery must be isolated: process one record
     * without changing the normal consumer-group position.
     */
    private void restoreOriginalCommittedOffsetsAfterSingleRecovery(final ConsumerRecord<String, byte[]> record,
                                                                    final String result) {
        if (!endpoint.isMessageRecoveryEnabled() || !endpoint.isRecoverySingleMessageOnly()) {
            return;
        }

        if (kafkaConsumer == null) {
            return;
        }

        // In assign mode the consumer left the group via unsubscribe().
        // commitSync on a group coordinator that already rebalanced will always fail with
        // "group has already rebalanced and assigned the partitions to another member".
        // The official group offset was never moved by the recovery seek anyway,
        // so skipping restore is correct and safe.
        if (assignMode) {
            LOG.warn("[SDIA Kafka] Message Recovery single-record mode completed in assign mode. "
                    + "Group offset restore skipped — consumer is not part of a subscribe() group."
                    + " | result=" + result
                    + " | recoveredOffset=" + (record == null ? -1L : record.offset()));
            return;
        }

        if (recoveryOriginalCommittedOffsets.isEmpty()) {
            LOG.warn("[SDIA Kafka] Message Recovery single-record mode completed without committing recovered offset. "
                    + "No original committed offset existed for this group/partition, so no offset restoration was required."
                    + " | result=" + result
                    + " | topic=" + (record == null ? "<unknown>" : record.topic())
                    + " | partition=" + (record == null ? -1 : record.partition())
                    + " | recoveredOffset=" + (record == null ? -1L : record.offset()));
            return;
        }

        try {
            final Map<TopicPartition, OffsetAndMetadata> restoreSnapshot =
                    new LinkedHashMap<TopicPartition, OffsetAndMetadata>(recoveryOriginalCommittedOffsets);

            LOG.warn("[SDIA Kafka] Message Recovery single-record mode restoring original committed offsets. "
                    + "Recovered record will NOT become the official consumer-group position."
                    + " | result=" + result
                    + " | recoveredTopic=" + (record == null ? "<unknown>" : record.topic())
                    + " | recoveredPartition=" + (record == null ? -1 : record.partition())
                    + " | recoveredOffset=" + (record == null ? -1L : record.offset())
                    + " | restoringOffsets=" + formatOffsets(restoreSnapshot));

            kafkaConsumer.commitSync(restoreSnapshot, cachedOffsetCommitTimeout);

            LOG.warn("[SDIA Kafka] Message Recovery original committed offsets restored. "
                    + "Normal consumption will resume from the previous official group offset after recovery is disabled."
                    + " | restoredOffsets=" + formatOffsets(restoreSnapshot));
        } catch (Throwable restoreEx) {
            final String msg = buildRecoveryRestoreError(rootCauseMessage(restoreEx));
            LOG.error("[SDIA Kafka] " + msg, restoreEx);
            throw new RuntimeCamelException(msg, restoreEx);
        }
    }

    /**
     * Publishes exactly one failed exchange for Stop on Error so the CPI Message Monitor
     * shows the consumed poison offset. This is intentionally separated from commit logic:
     * publishing the error is not a successful delivery and must never commit the Kafka offset.
     */
    private void publishStopErrorExchangeToIFlow(final Exchange exchange,
                                                 final ConsumerRecord<String, byte[]> record,
                                                 final String fatalMsg,
                                                 final Throwable cause,
                                                 final String failureType) {
        if (exchange == null) {
            LOG.warn("[SDIA Kafka] Stop on Error visibility skipped because exchange is null. failedOffset="
                    + (record == null ? "<unknown>" : String.valueOf(record.offset())));
            return;
        }

        try {
            exchange.getIn().setHeader("x-sdiakafka-error-handling", "Stop on Error");
            exchange.getIn().setHeader("x-sdiakafka-stop-on-error", Boolean.TRUE);
            exchange.getIn().setHeader("x-sdiakafka-stop-failure-type", failureType);
            exchange.getIn().setHeader("x-sdiakafka-offset-committed", Boolean.FALSE);
            exchange.getIn().setHeader("x-sdiakafka-next-offset-read", Boolean.FALSE);
            exchange.getIn().setHeader("x-sdiakafka-error-message", fatalMsg);
            exchange.setException(new RuntimeCamelException(fatalMsg));

            LOG.error("[SDIA Kafka] Stop on Error visible failure exchange published. Offset NOT committed. "
                    + "The next offset will NOT be read. group.id=" + effectiveGroupId
                    + " | topic=" + (record == null ? "<unknown>" : record.topic())
                    + " | partition=" + (record == null ? "<unknown>" : String.valueOf(record.partition()))
                    + " | failedOffset=" + (record == null ? "<unknown>" : String.valueOf(record.offset())));

            try {
                getProcessor().process(exchange);
            } catch (Throwable routeEx) {
                // Expected in Stop on Error: the iFlow may fail because the exchange carries an exception.
                // Do not rethrow; throwing here can make ScheduledPollConsumer/Camel restart the route and consume the next offset.
                LOG.warn("[SDIA Kafka] Stop on Error exchange delivered as failure; downstream route returned exception. "
                        + "Offset remains NOT committed. Detail: " + rootCauseMessage(routeEx));
            }
        } catch (Throwable visibilityEx) {
            LOG.warn("[SDIA Kafka] Stop on Error visibility publication failed. Offset remains NOT committed. Detail: "
                    + rootCauseMessage(visibilityEx));
        }
    }

    private void processPipelineExchangeWithPolicy(Exchange exchange) throws Exception {
        String handling = cachedErrorHandling;

        if (!"Retry Failed Message".equalsIgnoreCase(handling)) {
            getProcessor().process(exchange);
            Exception processorEx = exchange.getException();
            if (processorEx != null) throw processorEx;
            return;
        }

        int retries = endpoint.getRetryAttempts() != null ? endpoint.getRetryAttempts().intValue() : 5;
        if (retries < 5) retries = 5;
        if (retries > 10) retries = 10;

        Throwable lastFailure = null;
        int totalAttempts = retries + 1;

        for (int attempt = 1; attempt <= totalAttempts; attempt++) {
            try {
                exchange.setException(null);
                getProcessor().process(exchange);
                Exception processorEx = exchange.getException();
                if (processorEx != null) throw processorEx;
                if (attempt > 1) {
                    LOG.warn("[SDIA Kafka] Pipeline recovered on attempt " + attempt + " of " + totalAttempts + ".");
                }
                return;
            } catch (Throwable pipelineEx) {
                lastFailure = pipelineEx;
                if (attempt >= totalAttempts) break;

                long delaySec = Math.min(30L, (long) Math.pow(2, attempt - 1));
                LOG.warn("[SDIA Kafka] Pipeline attempt " + attempt + "/" + totalAttempts
                        + " failed. Retrying in " + delaySec + "s. Detail: " + rootCauseMessage(pipelineEx));
                try {
                    Thread.sleep(delaySec * 1000L);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeCamelException("Retry interrupted", pipelineEx);
                }
            }
        }

        LOG.error("[SDIA Kafka] All " + retries + " retry attempts exhausted. Offset NOT committed — adapter instance will stop silently until redeploy/restart to avoid an infinite reprocessing loop.");
        if (lastFailure instanceof Exception) {
            throw (Exception) lastFailure;
        }
        throw new RuntimeCamelException(lastFailure);
    }

    private String normalizeErrorHandling(String handling) {
        String h = handling == null ? "Stop on Error" : handling.trim();
        if (h.isEmpty()) return "Stop on Error";

        final String l = h.toLowerCase();
        if (l.indexOf("skip") >= 0) return "Skip Failed Message";
        if (l.indexOf("retry") >= 0) return "Retry Failed Message";
        if (l.indexOf("stop") >= 0) return "Stop on Error";
        return h;
    }

    private void applyAssignedStartOffsets(final List<TopicPartition> topicPartitions,
                                           final String reason) {
        if (kafkaConsumer == null || topicPartitions == null || topicPartitions.isEmpty()) {
            return;
        }

        final String reset = normalizeAutoOffsetReset(endpoint.getAutoOffsetReset());
        final Map<TopicPartition, Long> appliedPositions = new LinkedHashMap<TopicPartition, Long>();
        final List<TopicPartition> partitionsWithoutCommittedOffset = new ArrayList<TopicPartition>();

        try {
            for (TopicPartition tp : topicPartitions) {
                final OffsetAndMetadata committed = kafkaConsumer.committed(tp);
                if (committed != null) {
                    kafkaConsumer.seek(tp, committed.offset());
                    appliedPositions.put(tp, Long.valueOf(committed.offset()));
                } else {
                    partitionsWithoutCommittedOffset.add(tp);
                }
            }

            if (!partitionsWithoutCommittedOffset.isEmpty()) {
                if ("earliest".equalsIgnoreCase(reset)) {
                    kafkaConsumer.seekToBeginning(partitionsWithoutCommittedOffset);
                    for (TopicPartition tp : partitionsWithoutCommittedOffset) {
                        appliedPositions.put(tp, Long.valueOf(kafkaConsumer.position(tp)));
                    }
                } else if ("latest".equalsIgnoreCase(reset)) {
                    kafkaConsumer.seekToEnd(partitionsWithoutCommittedOffset);
                    for (TopicPartition tp : partitionsWithoutCommittedOffset) {
                        appliedPositions.put(tp, Long.valueOf(kafkaConsumer.position(tp)));
                    }
                } else {
                    throw new IllegalStateException("auto.offset.reset=none and no committed offset exists for "
                            + partitionsWithoutCommittedOffset
                            + ". Create/restore a committed offset for group.id=" + effectiveGroupId
                            + " or select EARLIEST/LATEST.");
                }
            }

            LOG.warn("[SDIA Kafka] Assigned consumer start position applied."
                    + " | reason=" + reason
                    + " | group.id=" + effectiveGroupId
                    + " | assignedPartitions=" + topicPartitions
                    + " | appliedPositions=" + appliedPositions
                    + " | auto.offset.reset=" + reset
                    + " | committedOffsetsUsed=" + (topicPartitions.size() - partitionsWithoutCommittedOffset.size())
                    + " | resetAppliedTo=" + partitionsWithoutCommittedOffset.size()
                    + " | onPremise=" + isOnPremiseProxyMode());
        } catch (Throwable t) {
            throw new RuntimeCamelException(
                    buildOffsetPositioningError(reason, reset, topicPartitions, rootCauseMessage(t)), t);
        }
    }

    private String buildOffsetPositioningError(final String reason,
                                               final String reset,
                                               final List<TopicPartition> topicPartitions,
                                               final String cause) {
        return buildCompactRuntimeError(
                "Assigned start offset positioning failed",
                "Event.Smart.Kafka.Offset.Positioning.Failed",
                "Bootstrap : " + safeValue(endpoint.resolveBootstrapServers())
                        + "\nGroup     : " + safeValue(effectiveGroupId)
                        + "\nReason    : " + safeValue(reason)
                        + "\nReset Mode: " + safeValue(reset)
                        + "\nPartitions: " + String.valueOf(topicPartitions),
                "The adapter could not determine a valid starting offset for one or more assigned partitions.",
                "1. Verify auto.offset.reset is set to EARLIEST or LATEST if no committed offset exists.\n"
                        + "2. Verify the consumer group has a valid committed offset, or reset it manually.\n"
                        + "3. Check broker connectivity for the partitions listed above.",
                cause);
    }

    /**
     * Applies controlled replay/recovery by timestamp.
     *
     * Critical design decision:
     * Message Recovery must not rely on subscribe()+poll() to obtain assignment.
     * A poll used only for assignment can fetch records and make the channel look
     * like a zombie in SAP CPI because the fetched records are not delivered to the
     * iFlow. For recovery we use deterministic assign()+offsetsForTimes()+seek().
     *
     * The Topic, Partition and Group ID still come from the Processing tab. The
     * Recovery tab only decides the replay position.
     */
    private String applyMessageRecoverySeekIfConfigured(final List<String> topicEntries,
                                                        final Map<String, List<PartitionInfo>> knownTopics,
                                                        final String partitionsParam) {
        final long seekTs = endpoint.getSeekToTimestampMs();
        if (!endpoint.isMessageRecoveryEnabled() || seekTs <= 0L || kafkaConsumer == null) {
            this.recoverySeekApplied = false;
            this.recoverySeekTimestampMs = -1L;
            this.recoverySeekInput = null;
            this.recoverySeekOffsets.clear();
            return null;
        }

        // "Read all from timestamp" — seek already registered as pendingSubscribeSeekTimestampMs
        // and will be applied via onPartitionsAssigned on the first poll(). The consumer stays
        // in the subscribe() group so normal offset commits work without rebalance failures.
        if (!endpoint.isRecoverySingleMessageOnly()) {
            LOG.warn("[SDIA Kafka] Read-all recovery: seek deferred to onPartitionsAssigned."
                    + " | timestampMs=" + seekTs
                    + " | group.id=" + effectiveGroupId
                    + " | assignMode unchanged=false");
            return null;
        }

        // "Read only first matched message" — uses assign() for deterministic single-record replay.
        this.recoverySeekApplied = false;
        this.recoverySeekTimestampMs = seekTs;
        this.recoverySeekInput = endpoint.getRecoveryTimestampValue();
        this.recoverySeekOffsets.clear();

        try {
            final List<TopicPartition> recoveryPartitions = resolveRecoveryTopicPartitions(topicEntries, knownTopics, partitionsParam);
            if (recoveryPartitions == null || recoveryPartitions.isEmpty()) {
                return buildRecoveryPartitionResolutionError(seekTs, partitionsParam);
            }

            captureOriginalCommittedOffsetsForSingleRecovery(recoveryPartitions);

            /*
             * Deterministic recovery mode.
             * Use assign() even when the normal channel uses subscribe(). This avoids
             * group-assignment timing and prevents an assignment poll from swallowing
             * records before the seek is applied.
             */
            try {
                kafkaConsumer.unsubscribe();
            } catch (Throwable ignored) {
                // Safe for an already assigned consumer.
            }
            kafkaConsumer.assign(recoveryPartitions);
            assignMode = true;

            final Map<TopicPartition, Long> timestamps = new LinkedHashMap<TopicPartition, Long>();
            for (TopicPartition tp : recoveryPartitions) {
                timestamps.put(tp, Long.valueOf(seekTs));
            }

            final Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndTimestamp> offsets =
                    kafkaConsumer.offsetsForTimes(timestamps);

            int applied = 0;
            final StringBuilder noOffsetPartitions = new StringBuilder();

            for (TopicPartition tp : recoveryPartitions) {
                final org.apache.kafka.clients.consumer.OffsetAndTimestamp oat = offsets != null ? offsets.get(tp) : null;
                if (oat != null) {
                    kafkaConsumer.seek(tp, oat.offset());
                    recoverySeekOffsets.put(tp, Long.valueOf(oat.offset()));
                    applied++;
                    LOG.warn("[SDIA Kafka] Message Recovery seek applied."
                            + " | topic=" + tp.topic()
                            + " | partition=" + tp.partition()
                            + " | timestampMs=" + seekTs
                            + " | offset=" + oat.offset());
                } else {
                    if (noOffsetPartitions.length() > 0) {
                        noOffsetPartitions.append(", ");
                    }
                    noOffsetPartitions.append(tp.topic()).append('-').append(tp.partition());
                    try {
                        kafkaConsumer.seekToEnd(Collections.singletonList(tp));
                        recoverySeekOffsets.put(tp, Long.valueOf(kafkaConsumer.position(tp)));
                    } catch (Throwable seekEndEx) {
                        LOG.warn("[SDIA Kafka] Message Recovery could not seekToEnd for " + tp
                                + ": " + rootCauseMessage(seekEndEx));
                    }
                }
            }

            if (applied <= 0) {
                return buildRecoveryNoOffsetError(seekTs, noOffsetPartitions.toString());
            }

            this.recoverySeekApplied = true;
            LOG.warn("[SDIA Kafka] Message Recovery seek completed."
                    + " | recoveryInput=" + recoverySeekInput
                    + " | recoveryTimestampMs=" + seekTs
                    + " | requestedPartitions=" + recoveryPartitions.size()
                    + " | appliedPartitions=" + applied
                    + " | noOffsetPartitions=" + noOffsetPartitions.toString()
                    + " | assignMode=true");
            return null;
        } catch (WakeupException wakeupEx) {
            throw wakeupEx;
        } catch (Throwable seekEx) {
            this.recoverySeekApplied = false;
            return buildRecoverySeekFailedError(seekTs, rootCauseMessage(seekEx));
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

        for (TopicPartition tp : recoveryPartitions) {
            if (tp == null) {
                continue;
            }
            try {
                final OffsetAndMetadata committed = kafkaConsumer.committed(tp);
                if (committed != null) {
                    recoveryOriginalCommittedOffsets.put(tp, committed);
                }
            } catch (Throwable commitLookupEx) {
                LOG.warn("[SDIA Kafka] Message Recovery could not read original committed offset."
                        + " | group.id=" + effectiveGroupId
                        + " | topic=" + tp.topic()
                        + " | partition=" + tp.partition()
                        + " | error=" + rootCauseMessage(commitLookupEx));
            }
        }

        LOG.warn("[SDIA Kafka] Message Recovery captured original committed offsets before isolated seek."
                + " | group.id=" + effectiveGroupId
                + " | offsets=" + formatOffsets(recoveryOriginalCommittedOffsets));
    }

    private List<TopicPartition> resolveRecoveryTopicPartitions(final List<String> topicEntries,
                                                                final Map<String, List<PartitionInfo>> knownTopics,
                                                                final String partitionsParam) {
        final List<TopicPartition> result = new ArrayList<TopicPartition>();
        if (topicEntries == null || topicEntries.isEmpty()) {
            return result;
        }

        if (partitionsParam != null && partitionsParam.trim().length() > 0) {
            if (topicEntries.size() == 1 && !isPatternMode(topicEntries.get(0))) {
                return parsePartitions(topicEntries.get(0), partitionsParam);
            }
            return result;
        }

        final List<String> resolvedTopics = new ArrayList<String>();
        if (containsPatternMode(topicEntries)) {
            final Pattern p = compileTopicPatternExpression(topicEntries);
            if (knownTopics != null) {
                for (String topic : knownTopics.keySet()) {
                    if (topic != null && p.matcher(topic).matches()) {
                        resolvedTopics.add(topic);
                    }
                }
            }
        } else {
            resolvedTopics.addAll(topicEntries);
        }

        for (String topic : resolvedTopics) {
            if (topic == null || topic.trim().length() == 0) {
                continue;
            }
            List<PartitionInfo> partitions = knownTopics != null ? knownTopics.get(topic) : null;
            if (partitions == null || partitions.isEmpty()) {
                try {
                    partitions = kafkaConsumer.partitionsFor(topic);
                } catch (Throwable lookupEx) {
                    LOG.warn("[SDIA Kafka] Message Recovery could not lookup partitions for topic=" + topic
                            + " | error=" + rootCauseMessage(lookupEx));
                    partitions = null;
                }
            }
            if (partitions == null || partitions.isEmpty()) {
                continue;
            }
            for (PartitionInfo pi : partitions) {
                if (pi != null) {
                    result.add(new TopicPartition(pi.topic(), pi.partition()));
                }
            }
        }
        return result;
    }

    private void commitSuccessOffsets(Map<TopicPartition, MutableOffset> successOffsets, int delivered) {
        if (successOffsets == null || successOffsets.isEmpty() || closed.get() || kafkaConsumer == null) return;

        pollCommitOffsets.clear();
        for (Map.Entry<TopicPartition, MutableOffset> e : successOffsets.entrySet()) {
            final MutableOffset offset = e.getValue();
            if (offset != null) {
                pollCommitOffsets.put(e.getKey(), new OffsetAndMetadata(offset.nextOffset));
            }
        }
        if (pollCommitOffsets.isEmpty()) return;

        final Map<TopicPartition, OffsetAndMetadata> commitSnapshot = pollCommitOffsets;

        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug("[SDIA Kafka] COMMIT REQUEST group.id=" + effectiveGroupId
                        + " | delivered=" + delivered
                        + " | offsets=" + formatOffsets(commitSnapshot));
            }

            kafkaConsumer.commitSync(commitSnapshot, cachedOffsetCommitTimeout);

            if (LOG.isDebugEnabled()) {
                LOG.debug("[SDIA Kafka] COMMIT OK group.id=" + effectiveGroupId
                        + " | offsets=" + formatOffsets(commitSnapshot));
            }

            registerRuntimeStatus("Event.Smart.Kafka.Active.LastCommitOk.Records." + delivered, null, null);
        } catch (Throwable commitEx) {
            String errorMsg = buildOffsetCommitFailureError(formatOffsets(commitSnapshot), rootCauseMessage(commitEx));

            LOG.error("[SDIA Kafka] " + errorMsg, commitEx);
            try { surfaceErrorToChannel(errorMsg); } catch (Throwable ignored) {}

            closeKafkaConsumerSilently();
            this.initialized = false;
            this.fatalInitFailure = true;
            this.nonRetryableConfigurationFailure = true;
            this.fatalInitReason = errorMsg;

            throw new RuntimeCamelException(errorMsg, commitEx);
        }
    }

    private void commitAvroPoisonOffsetThenRecycle(ConsumerRecord<String, byte[]> record, String fatalMsg, Throwable conversionEx) {
        TopicPartition tp = topicPartition(record);
        OffsetAndMetadata nextOffset = new OffsetAndMetadata(record.offset() + 1);
        Map<TopicPartition, OffsetAndMetadata> poisonCommit =
                new LinkedHashMap<TopicPartition, OffsetAndMetadata>();
        poisonCommit.put(tp, nextOffset);

        registerRuntimeStatus("Event.Smart.Kafka.Avro.PoisonCommitted", record, conversionEx);
        LOG.error("[SDIA Kafka] " + oneLineForLog(fatalMsg) + " | rootCause=" + safeLine(rootCauseMessage(conversionEx), 180));

        try {
            SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), fatalMsg);
        } catch (Throwable ignored) {}

        try {
            kafkaConsumer.commitSync(poisonCommit, cachedOffsetCommitTimeout);
            LOG.warn("[SDIA Kafka] Avro poison pill committed and skipped. group.id=" + effectiveGroupId
                    + " | topic=" + record.topic()
                    + " | partition=" + record.partition()
                    + " | failedOffset=" + record.offset()
                    + " | committedOffset=" + (record.offset() + 1));

            if (shouldStopAfterRecoveredRecord(record)) {
                restoreOriginalCommittedOffsetsAfterSingleRecovery(record, "skipped after conversion failure");
            }
        } catch (Throwable commitEx) {
            String commitMsg = buildAvroPoisonCommitFailureError(record, rootCauseMessage(commitEx));

            LOG.error("[SDIA Kafka] " + commitMsg, commitEx);
            try { SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), commitMsg); } catch (Throwable ignored) {}
            throw new RuntimeCamelException(commitMsg, commitEx);
        } finally {
            // Force a clean next poll. This avoids staying inside a poll batch that already contained
            // records after the poison pill and guarantees restart from the committed offset + 1.
            this.initialized = false;
            this.fatalAvroConversionFailure = false;
            this.fatalAvroConversionReason = null;
            this.nextAvroRetryAllowedAtMs = 0L;
            closeKafkaConsumerSilently();
        }
    }

    private void haltConsumerAfterFatalAvroFailure(ConsumerRecord<String, byte[]> record, String reason, Throwable cause) {
        final String handling = cachedErrorHandling;

        try {
            SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), reason);
        } catch (Throwable ignored) {}

        if ("Stop on Error".equalsIgnoreCase(handling)) {
            this.stoppedByErrorPolicy = true;
            this.stoppedByErrorPolicyReason = reason;
            this.fatalAvroConversionFailure = false;
            this.fatalAvroConversionReason = null;
            this.nextAvroRetryAllowedAtMs = 0L;

            stopConsumerForStopOnError(record, cause, "Avro conversion failure");
            return;
        }

        // Retry Failed Message is bounded. After the configured retry path reaches this deterministic
        // conversion failure, park the adapter without committing the failed offset. Retrying the same
        // invalid bytes forever only creates an operational loop.
        this.stoppedByErrorPolicy = true;
        this.stoppedByErrorPolicyReason = reason;
        this.fatalAvroConversionFailure = false;
        this.fatalAvroConversionReason = null;
        this.nextAvroRetryAllowedAtMs = 0L;

        stopConsumerForTerminalFailure(record, cause, "Avro conversion failure after Retry Failed Message", "Retry Failed Message");
    }

    private void haltConsumerAfterPipelineFailure(ConsumerRecord<String, byte[]> record, String reason, Throwable cause) {
        final String handling = cachedErrorHandling;

        try {
            SdiaKafkaEndpointInformationService.register(endpoint.getTopicPattern(), reason);
        } catch (Throwable ignored) {}

        if ("Stop on Error".equalsIgnoreCase(handling)) {
            this.stoppedByErrorPolicy = true;
            this.stoppedByErrorPolicyReason = reason;
            stopConsumerForStopOnError(record, cause, "Pipeline processor failure");
            return;
        }

        if ("Retry Failed Message".equalsIgnoreCase(handling)) {
            this.stoppedByErrorPolicy = true;
            this.stoppedByErrorPolicyReason = reason;
            stopConsumerForTerminalFailure(record, cause, "Pipeline processor failure after Retry Failed Message exhaustion", "Retry Failed Message");
            return;
        }

        this.initialized = false;

        try {
            if (kafkaConsumer != null && record != null) {
                TopicPartition tp = topicPartition(record);
                kafkaConsumer.seek(tp, record.offset());
                LOG.warn("[SDIA Kafka] Pipeline failure policy — offset NOT committed. Seeked back to offset "
                        + record.offset() + " on " + record.topic() + "-" + record.partition() + ".");
            }
        } catch (Throwable seekEx) {
            LOG.warn("[SDIA Kafka] Could not seek back after pipeline failure: " + rootCauseMessage(seekEx));
        }

        closeKafkaConsumerSilently();
    }

    private void stopConsumerForStopOnError(final ConsumerRecord<String, byte[]> record,
                                            final Throwable cause,
                                            final String failureType) {
        stopConsumerForTerminalFailure(record, cause, failureType, "Stop on Error");
    }

    private void stopConsumerForTerminalFailure(final ConsumerRecord<String, byte[]> record,
                                                final Throwable cause,
                                                final String failureType,
                                                final String policyName) {
        final String policy = policyName == null ? "Terminal Error" : policyName;
        try {
            if (kafkaConsumer != null && record != null) {
                TopicPartition failedPartition = topicPartition(record);
                try {
                    kafkaConsumer.seek(failedPartition, record.offset());
                } catch (Throwable seekEx) {
                    LOG.warn("[SDIA Kafka] " + policy + " could not seek back to failed offset before shutdown: "
                            + rootCauseMessage(seekEx));
                }

                LOG.error("[SDIA Kafka] " + policy + " policy activated. Offset NOT committed. "
                                + "KafkaConsumer will be closed and this deployed adapter instance will stay silent until redeploy/restart. "
                                + "This avoids infinite reprocessing loops and paused-heartbeat zombie consumers holding the partition assignment. "
                                + "failureType=" + failureType
                                + " | topic=" + record.topic()
                                + " | partition=" + record.partition()
                                + " | failedOffset=" + record.offset(),
                        cause);
            } else {
                LOG.error("[SDIA Kafka] " + policy + " policy activated without active KafkaConsumer/record. "
                        + "Offset was NOT committed. Adapter instance is stopped until redeploy/restart.", cause);
            }
        } catch (Throwable stopEx) {
            LOG.warn("[SDIA Kafka] " + policy + " shutdown handler failed; adapter remains logically stopped: "
                    + rootCauseMessage(stopEx));
        } finally {
            // Critical CPI/ADK behavior:
            // Close the consumer instead of keeping paused heartbeat polls. A paused live consumer can
            // retain the partition across redeploy/config changes and produce the observed 5-10 minute
            // delay. Do not throw after this method; poll() returns and the scheduler remains quiet.
            this.initialized = false;
            closeKafkaConsumerSilently();
        }
    }

    private Exchange processRecord(final ConsumerRecord<String, byte[]> record) {
        Exchange exchange = endpoint.createExchange();

        exchange.getIn().setHeader("x-sdiakafka-topic",      record.topic());
        exchange.getIn().setHeader("x-sdiakafka-partition",  record.partition());
        exchange.getIn().setHeader("x-sdiakafka-offset",     record.offset());
        exchange.getIn().setHeader("x-sdiakafka-key",        record.key());
        exchange.getIn().setHeader("x-sdiakafka-timestamp",  record.timestamp());
        if (endpoint.isMessageRecoveryEnabled()) {
            final TopicPartition recoveryTp = topicPartition(record);
            exchange.getIn().setHeader("x-sdiakafka-recovery-mode", endpoint.getRecoveryMode());
            exchange.getIn().setHeader("x-sdiakafka-seek-input", recoverySeekInput);
            exchange.getIn().setHeader("x-sdiakafka-seek-timestamp-ms", recoverySeekTimestampMs);
            exchange.getIn().setHeader("x-sdiakafka-seek-applied", recoverySeekApplied);
            exchange.getIn().setHeader("x-sdiakafka-seek-offset", recoverySeekOffsets.get(recoveryTp));
            exchange.getIn().setHeader("x-sdiakafka-recovery-read-mode", endpoint.getRecoveryReadMode());
            exchange.getIn().setHeader("x-sdiakafka-recovery-single-message-completed", recoverySingleMessageCompleted);
        }

        final byte[] payloadBytes = record.value();
        final int payloadLength = (payloadBytes == null) ? 0 : payloadBytes.length;

        final String conversionFormat = cachedConversionFormat;
        final String schemaRegistryUrl = cachedSchemaRegistryUrl;
        final String registryCredentialAlias = cachedRegistryCredentialAlias;
        final boolean conversionRequested = cachedConversionRequested;

        if (cachedJsonSchemaPassthrough) {
            exchange.getIn().setHeader(org.apache.camel.Exchange.CONTENT_TYPE, "application/json");
            exchange.getIn().setHeader("x-sdiakafka-conversionFormat", "JSON_SCHEMA");
            exchange.getIn().setBody(payloadBytes != null ? new String(payloadBytes, java.nio.charset.StandardCharsets.UTF_8) : "");
            return exchange;
        }

        exchange.getIn().setHeader("x-sdiakafka-conversionFormat", conversionRequested ? conversionFormat : "None");

        if (conversionRequested) {
            final long maxPayloadBytes = cachedMaxPayloadBytes;
            if (payloadLength > maxPayloadBytes) {
                String detail = "Payload size " + payloadLength + " bytes exceeds the configured Avro conversion limit of "
                        + cachedMaxPayloadSizeMb + " MB."
                        + " With conversion disabled (None), the adapter passes raw bytes and this conversion limit is not applied."
                        + " For larger events, use external storage and send a pointer event instead.";
                return markError(exchange, payloadBytes, "Event.Smart.Kafka.Payload.TooLarge", conversionFormat, "n/a", -1, detail);
            }

            if (schemaRegistryUrl == null) {
                return markError(exchange, payloadBytes, "Event.Smart.Kafka.Config.Missing.RegistryHost", conversionFormat, "n/a", -1, "Schema Registry Host Address is mandatory.");
            }
            if (registryCredentialAlias == null) {
                return markError(exchange, payloadBytes, "Event.Smart.Kafka.Config.Missing.Credential", conversionFormat, "n/a", -1, "Schema Registry Credential Alias is mandatory.");
            }
            if (payloadBytes == null || payloadLength == 0) {
                return markError(exchange, payloadBytes, "Event.Smart.Kafka.Empty.Payload", conversionFormat, "n/a", -1, "Payload is empty.");
            }

            int resolvedSchemaId = -1;
            String effectiveSchemaMode = "Unresolved";
            boolean schemaIdFromMagicByte = false;

            try {
                final String fixedSchemaIdStr = cachedFixedSchemaIdStr;
                schemaIdFromMagicByte = shouldUseMagicByte(cachedSchemaIdResolutionMode, fixedSchemaIdStr, payloadBytes);

                int schemaId = 0;
                Integer fixedSchemaIdObj = null;

                if (schemaIdFromMagicByte) {
                    schemaId = SdiaKafkaAvroConverter.extractSchemaId(payloadBytes);
                    effectiveSchemaMode = "MagicByte";

                    // Optional safety guard: in Magic Byte mode the Schema ID is read
                    // from the Kafka record header. If the channel also contains a
                    // numeric schemaRegistrySchemaId, treat it as an EXPECTED schema id.
                    // This catches the common operator mistake: a 2 MB payload produced
                    // with schema B reaches a channel visually configured for schema A.
                    if (fixedSchemaIdStr != null) {
                        if (cachedFixedSchemaIdInvalid || cachedFixedSchemaId == null) {
                            return markError(exchange, payloadBytes, "Event.Smart.Kafka.Invalid.ExpectedSchemaID.Format", conversionFormat, "MagicByte", schemaId, "Magic Byte mode received a non-numeric expected Schema ID in schemaRegistrySchemaId. Configured value: " + fixedSchemaIdStr);
                        }
                        final int expectedSchemaId = cachedFixedSchemaId.intValue();
                        if (expectedSchemaId != schemaId) {
                            return markError(exchange, payloadBytes, "Event.Smart.Kafka.Avro.Magic.SchemaID.Mismatch", conversionFormat, "MagicByte", schemaId, "Magic Byte payload Schema ID mismatch. Channel expected Schema ID=" + expectedSchemaId + ", but Kafka record wire header contains Schema ID=" + schemaId + ". This usually means the channel is configured for another payload/schema, for example 1 MB schema while the producer sent a 2 MB schema.");
                        }
                    }
                } else {
                    if (fixedSchemaIdStr != null) {
                        if (cachedFixedSchemaIdInvalid || cachedFixedSchemaId == null) {
                            return markError(exchange, payloadBytes, "Event.Smart.Kafka.Invalid.SchemaID.Format", conversionFormat, "FixedSchemaID", -1, "Fixed Schema ID must be a positive numeric value. Configured value: " + fixedSchemaIdStr);
                        }
                        fixedSchemaIdObj = cachedFixedSchemaId;
                        schemaId = fixedSchemaIdObj.intValue();
                        effectiveSchemaMode = "FixedSchemaID";
                    } else {
                        throw new IllegalArgumentException("CONFIG ERROR: Schema ID resolution failed. The message does not contain a Magic Byte header, and no fallback Fixed Schema ID is defined on the CPI Channel.");
                    }
                }
                resolvedSchemaId = schemaId;

                SdiaKafkaCredentials credentials = SdiaKafkaCredentialsResolver.resolve(registryCredentialAlias);

                String converted = SdiaKafkaAvroConverter.convert(
                        payloadBytes,
                        conversionFormat,
                        schemaRegistryUrl,
                        credentials.getUsername(),
                        credentials.getPassword(),
                        schemaIdFromMagicByte,
                        fixedSchemaIdObj != null ? fixedSchemaIdObj : Integer.valueOf(resolvedSchemaId),
                        cachedSchemaCacheMaxBytes,
                        cachedSchemaRegistryConnectTimeoutMs,
                        cachedSchemaRegistryReadTimeoutMs
                );

                if (converted == null || converted.length() == 0) {
                    return markError(exchange, payloadBytes, "Event.Smart.Kafka.Avro.Empty.Conversion.Result", conversionFormat, effectiveSchemaMode, resolvedSchemaId, "Avro conversion produced an empty body. The adapter blocked the message instead of passing a blank payload silently. Verify that the Schema ID belongs to this exact payload type and size.");
                }

                exchange.getIn().setHeader("SAP_KafkaSchemaId",                                   schemaId);
                exchange.getIn().setHeader("SAP_KafkaSchemaRegistryUrl",                          schemaRegistryUrl);
                exchange.getIn().setHeader("SAP_KafkaSchemaRegistryCredentialAlias",              registryCredentialAlias);
                exchange.getIn().setHeader("x-sdiakafka-schema-mode",                             effectiveSchemaMode);
                exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "XML".equalsIgnoreCase(conversionFormat) ? "application/xml" : "application/json");
                exchange.getIn().setBody(converted);

            } catch (Throwable t) {
                String errorCode;
                String rootMsg = rootCauseMessage(t);
                if (rootMsg != null && rootMsg.contains("body=")) {
                    rootMsg = rootMsg.substring(rootMsg.indexOf("body="));
                }

                String m = (rootMsg == null ? "" : rootMsg.toLowerCase());
                if (m.contains("schema not found") || m.contains("404")) {
                    errorCode = "Event.Smart.Kafka.Avro.Registry.NotFound";
                } else if (m.contains("authentication") || m.contains("401") || m.contains("403")) {
                    errorCode = "Event.Smart.Kafka.Avro.Registry.Auth.Failure";
                } else if (m.contains("schema id mismatch") || m.contains("schemaid mismatch") || m.contains("wire header schema id")) {
                    errorCode = schemaIdFromMagicByte ? "Event.Smart.Kafka.Avro.Magic.SchemaID.Mismatch" : "Event.Smart.Kafka.Avro.Fixed.SchemaID.Mismatch";
                } else if (m.contains("empty body") || m.contains("empty conversion")) {
                    errorCode = "Event.Smart.Kafka.Avro.Empty.Conversion.Result";
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
            exchange.getIn().setHeader(Exchange.CONTENT_TYPE, "application/octet-stream");
            exchange.getIn().setBody(payloadBytes);
        }

        return exchange;
    }

    private void applyConsumerRuntimeProperties(final Properties props, final boolean adminMode) {
        final int requestTimeoutMs = safeSecondsToMillis(endpoint.getRequestTimeoutS(), 30);
        final int sessionTimeoutMs = safeSecondsToMillis(endpoint.getSessionTimeoutS(), 10);
        final int heartbeatIntervalMs = safeSecondsToMillis(endpoint.getHeartbeatIntervalS(), 3);
        final int reconnectBackoffMs = safeSecondsToMillis(endpoint.getReconnectDelayS(), 1);

        props.put("request.timeout.ms", String.valueOf(requestTimeoutMs));
        props.put("default.api.timeout.ms", String.valueOf(adminMode ? Math.min(requestTimeoutMs, 10000) : requestTimeoutMs));
        props.put("max.block.ms", String.valueOf(adminMode ? Math.min(requestTimeoutMs, 10000) : requestTimeoutMs));
        props.put("metadata.max.age.ms", "10000");

        props.put("fetch.min.bytes", String.valueOf(endpoint.getMinFetchSizeBytes()));

        int effectiveFetchMaxBytes = endpoint.getMaxFetchSizeBytes().intValue();
        int effectivePartitionFetchBytes = endpoint.getMaxPartitionFetchSizeBytes().intValue();
        if (isAvroConversionEnabledForFetchGuard()) {
            final int avroFetchFloor = computeAvroOversizeDetectionFetchFloor(endpoint.getMaxPayloadSizeBytes());
            if (effectiveFetchMaxBytes < avroFetchFloor) {
                effectiveFetchMaxBytes = avroFetchFloor;
            }
            if (effectivePartitionFetchBytes < avroFetchFloor) {
                effectivePartitionFetchBytes = avroFetchFloor;
            }
        }
        props.put("fetch.max.bytes", String.valueOf(effectiveFetchMaxBytes));
        props.put("max.partition.fetch.bytes", String.valueOf(effectivePartitionFetchBytes));
        int effectiveFetchMaxWaitMs = endpoint.getMaxFetchWaitTime();
        if (isOnPremiseProxyMode() && effectiveFetchMaxWaitMs > 1000) {
            effectiveFetchMaxWaitMs = 1000;
        }
        props.put("fetch.max.wait.ms", String.valueOf(effectiveFetchMaxWaitMs));
        if (isOnPremiseProxyMode()) {
            props.put("client.dns.lookup", "use_all_dns_ips");
            props.put("connections.max.idle.ms", "30000");
        }

        final String runtimeHandling = normalizeErrorHandling(endpoint.getErrorHandling());
        final int configuredMaxPollRecords = endpoint.getMaxPollRecords() != null ? endpoint.getMaxPollRecords().intValue() : 1;
        final int effectiveMaxPollRecords = configuredMaxPollRecords;
        props.put("max.poll.records", String.valueOf(effectiveMaxPollRecords));
        // NOTE: max.poll.records limits how many records are DELIVERED per poll() call, not how many
        // are fetched from the broker. Kafka pre-fetches data into an internal buffer regardless of this
        // setting. Setting max.poll.records=1 will process one record per poll cycle but the broker
        // fetch may already have more in buffer — they will be delivered in subsequent poll() calls.
        // This is standard Kafka KafkaConsumer behavior, not an adapter limitation.

        props.put("session.timeout.ms", String.valueOf(sessionTimeoutMs));
        props.put("heartbeat.interval.ms", String.valueOf(heartbeatIntervalMs));
        props.put("max.poll.interval.ms", String.valueOf(endpoint.getMaxPollIntervalMs()));
        props.put("reconnect.backoff.ms", String.valueOf(reconnectBackoffMs));
        props.put("reconnect.backoff.max.ms", String.valueOf(reconnectBackoffMs));
        props.put("retry.backoff.ms", String.valueOf(endpoint.getMaxRetryBackoffMs()));

        // Manual offset ownership. CPI processing success must happen before commit.
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, normalizeAutoOffsetReset(endpoint.getAutoOffsetReset()));

        LOG.warn("[SDIA Kafka] Runtime offset policy: group.id="
                + props.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
                + " | error.handling="
                + runtimeHandling
                + " | enable.auto.commit="
                + props.getProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG)
                + " | auto.offset.reset="
                + props.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG)
                + " | fetch.min.bytes="
                + props.getProperty("fetch.min.bytes")
                + " | fetch.max.bytes="
                + props.getProperty("fetch.max.bytes")
                + " | max.partition.fetch.bytes="
                + props.getProperty("max.partition.fetch.bytes")
                + " | conversion.limit.bytes="
                + endpoint.getMaxPayloadSizeBytes()
                + " | max.poll.records="
                + props.getProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    private void logEmptyPollHeartbeat() {
        long now = System.currentTimeMillis();
        if (now - lastEmptyPollLogAtMs < EMPTY_POLL_DIAG_LOG_EVERY_MS) {
            return;
        }
        lastEmptyPollLogAtMs = now;

        try {
            java.util.Set<TopicPartition> assignment = kafkaConsumer != null
                    ? kafkaConsumer.assignment()
                    : java.util.Collections.<TopicPartition>emptySet();
            java.util.Set<String> subscription = kafkaConsumer != null
                    ? kafkaConsumer.subscription()
                    : java.util.Collections.<String>emptySet();

            // ── New group + LATEST detection ──────────────────────────────────
            // If the consumer group has no committed offsets on any assigned partition
            // and auto.offset.reset=latest, messages already in the topic will be
            // silently skipped. Warn the operator so they know what to do.
            final String offsetReset = normalizeAutoOffsetReset(endpoint.getAutoOffsetReset());
            if ("latest".equalsIgnoreCase(offsetReset)
                    && !assignment.isEmpty()
                    && kafkaConsumer != null) {
                try {
                    boolean hasNoCommittedOffset = true;
                    for (TopicPartition tp : assignment) {
                        org.apache.kafka.clients.consumer.OffsetAndMetadata committed =
                                kafkaConsumer.committed(tp);
                        if (committed != null) {
                            hasNoCommittedOffset = false;
                            break;
                        }
                    }
                    if (hasNoCommittedOffset) {
                        LOG.warn("[SDIA Kafka] ⚠️ NEW GROUP DETECTED — no committed offset found for any partition."
                                + " auto.offset.reset=latest means messages already in the topic will NOT be read."
                                + " If you want to read existing messages, change auto.offset.reset to EARLIEST"
                                + " and redeploy the iFlow."
                                + " | group.id=" + effectiveGroupId
                                + " | assignment=" + assignment
                                + " | hint=Change 'Auto Offset Reset' to EARLIEST in the Processing tab.");
                    }
                } catch (Throwable offsetCheckEx) {
                    // Non-critical — ignore offset check errors
                }
            }

            LOG.warn("[SDIA Kafka] EMPTY POLL heartbeat group.id=" + effectiveGroupId
                    + " | initialized=" + initialized
                    + " | auto.offset.reset=" + offsetReset
                    + " | subscription=" + subscription
                    + " | assignment=" + assignment
                    + " | topicPattern=" + endpoint.getTopicPattern());
        } catch (Throwable t) {
            LOG.warn("[SDIA Kafka] EMPTY POLL heartbeat failed: " + rootCauseMessage(t));
        }
    }

    private boolean isAvroConversionEnabledForFetchGuard() {
        final String f = normalizeConversionFormat(endpoint.getEffectiveConversionFormat());
        return f != null && !"None".equalsIgnoreCase(f) && !"JSON_SCHEMA".equalsIgnoreCase(f);
    }

    private static int computeAvroOversizeDetectionFetchFloor(final long maxPayloadBytes) {
        long floor = maxPayloadBytes + (long) AVRO_OVERSIZE_DETECTION_MARGIN_BYTES;
        if (floor < (long) AVRO_OVERSIZE_DETECTION_FETCH_FLOOR_BYTES) {
            floor = (long) AVRO_OVERSIZE_DETECTION_FETCH_FLOOR_BYTES;
        }
        return floor > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) floor;
    }

    private boolean isAdapterGeneratedAvroException(final Throwable ex) {
        if (!(ex instanceof RuntimeCamelException)) {
            return false;
        }
        final String message = ex.getMessage();
        if (message == null) {
            return false;
        }
        final String m = message.toLowerCase(java.util.Locale.ROOT);
        return m.indexOf("event.smart.kafka.avro") >= 0
                || m.indexOf("avro conversion") >= 0
                || m.indexOf("fixed schema id") >= 0
                || m.indexOf("magic byte") >= 0
                || m.indexOf("schema registry") >= 0
                || m.indexOf("schemaid") >= 0
                || m.indexOf("schema id") >= 0;
    }

    private String normalizeAutoOffsetReset(final String configured) {
        final String value = configured == null ? "earliest" : configured.trim();
        if ("earliest".equalsIgnoreCase(value)) return "earliest";
        if ("latest".equalsIgnoreCase(value)) return "latest";
        if ("none".equalsIgnoreCase(value)) return "none";

        LOG.warn("[SDIA Kafka] Invalid autoOffsetReset value [" + configured
                + "]. Falling back to earliest.");
        return "earliest";
    }

    private String formatOffsets(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (offsets == null || offsets.isEmpty()) {
            return "{}";
        }

        StringBuilder sb = new StringBuilder(128);
        sb.append('{');
        boolean first = true;
        for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            TopicPartition tp = e.getKey();
            OffsetAndMetadata om = e.getValue();
            sb.append(tp.topic()).append('-').append(tp.partition())
                    .append("=>").append(om == null ? "null" : String.valueOf(om.offset()));
        }
        sb.append('}');
        return sb.toString();
    }

    private static int safeSecondsToMillis(final Long seconds, final int defaultSeconds) {
        final long sec = (seconds == null || seconds.longValue() <= 0L) ? defaultSeconds : seconds.longValue();
        final long ms = sec * 1000L;
        return ms > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) ms;
    }

    private String buildKafkaBrokerConnectionConfigurationError(final String cause, final String bootstrapServers) {
        final String root = safeRoot(cause);
        final String code = classifyBrokerStartupCode(root);
        final String title = titleForCode(code);
        final String context = commonKafkaContext(bootstrapServers)
                + "\nSecurity   : " + describeSecurityProfile()
                + "\nCredential : " + describeCredentialAlias();
        return buildCompactRuntimeError(
                title,
                code,
                context,
                causeForCode(code),
                fixForCode(code, root),
                root);
    }

    private String buildAdapterConfigurationError(final IllegalArgumentException cfgEx) {
        final String raw = cfgEx == null || cfgEx.getMessage() == null
                ? "Unknown adapter configuration error."
                : cfgEx.getMessage();
        final String clean = stripSdiaPrefix(raw);
        final String lower = clean.toLowerCase(java.util.Locale.ROOT);
        final String configuredValue = extractAfter(clean, "got:");

        if (lower.indexOf("schemaregistryschemaid") >= 0
                || lower.indexOf("fixed schema id") >= 0
                || lower.indexOf("schema id") >= 0) {
            final String context = "Field      : schemaRegistrySchemaId"
                    + "\nConversion : " + normalizeConversionFormat(endpoint.getEffectiveConversionFormat())
                    + "\nSchemaMode : " + safeValue(endpoint.getSchemaIdResolutionMode())
                    + "\nValue      : " + (configuredValue == null ? "<empty>" : configuredValue)
                    + "\nBootstrap  : " + safeValue(endpoint.resolveBootstrapServers());
            return buildCompactRuntimeError(
                    "Invalid fixed Schema Registry ID",
                    "Event.Smart.Kafka.Config.Invalid.SchemaID",
                    context,
                    "The configured Fixed Schema ID is not a positive numeric Schema Registry ID.",
                    "1. Use a numeric schema ID, for example 1, 17 or 100010.\n"
                            + "2. Do not use a subject name such as order.created-value.\n"
                            + "3. If the payload has Confluent wire format, use Schema ID Source = Magic Byte.",
                    clean);
        }

        return buildCompactRuntimeError(
                "Invalid adapter channel configuration",
                "Event.Smart.Kafka.Config.Invalid.Channel",
                commonKafkaContext(endpoint.resolveBootstrapServers()),
                "The adapter channel contains an invalid or unsupported configuration value.",
                "1. Review the field named in the root cause.\n"
                        + "2. Correct the channel configuration.\n"
                        + "3. Redeploy the iFlow; this startup error is non-retryable.",
                clean);
    }

    private String buildMissingBrokerCredentialError() {
        return buildCompactRuntimeError(
                "Kafka broker credential is missing",
                "Event.Smart.Kafka.Config.Credential.Missing",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nSecurity   : " + describeSecurityProfile()
                        + "\nCredential : <empty>",
                "SASL authentication requires a CPI Security Material alias.",
                "1. Create a CPI Security Material entry of type User Credentials.\n"
                        + "2. Put its exact alias in the adapter Credential Alias field.\n"
                        + "3. If the broker has no authentication, set Authentication = NONE.",
                "Credential Alias is empty while Authentication is SASL.");
    }

    private String buildBrokerCredentialResolutionError(final String alias, final String cause) {
        return buildCompactRuntimeError(
                "Kafka broker credential could not be loaded",
                "Event.Smart.Kafka.Config.Credential.NotFound",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nSecurity   : " + describeSecurityProfile()
                        + "\nCredential : " + safeValue(alias),
                "The configured CPI Security Material alias could not be resolved.",
                "1. Verify the alias exists in CPI Security Materials.\n"
                        + "2. Verify the type is User Credentials for SASL.\n"
                        + "3. Verify the channel alias matches exactly, including case.",
                safeRoot(cause));
    }

    private String buildBootstrapServersMissingError() {
        return buildCompactRuntimeError(
                "Kafka bootstrap servers are missing",
                "Event.Smart.Kafka.Config.Bootstrap.Missing",
                commonKafkaContext("<empty>"),
                "No Kafka bootstrap host and port were configured.",
                "1. Add at least one bootstrap host:port row.\n"
                        + "2. For Cloud Connector, use the virtual host and virtual port.\n"
                        + "3. Redeploy after correcting the channel.",
                "kafkaClusterHostsTable is empty and no fallback host was provided.");
    }

    private String buildCloudConnectorTunnelError(final String bootstrapServers, final String cause) {
        final String root = safeRoot(cause);
        final String code = classifyCloudConnectorTunnelCode(root);
        return buildCompactRuntimeError(
                titleForCloudConnectorTunnelCode(code),
                code,
                commonKafkaContext(bootstrapServers)
                        + "\nLocation  : " + displayOptionalLocationId(endpoint.getSapSapccLocationId()),
                causeForCloudConnectorTunnelCode(code),
                fixForCloudConnectorTunnelCode(code),
                root);
    }

    private boolean isNonRetryableCloudConnectorTunnelFailure(final String cause) {
        final String l = lowerSafe(cause);

        // Hard configuration errors — no amount of retrying will fix these without redeploy.
        if (containsAny(l,
                "address already in use",
                "local port is already in use",
                "not registered in the current osgi relay registry",
                "invalid kafka bootstrap entry",
                "invalid kafka bootstrap port",
                "proxyhost must not be empty",
                "invalid proxyport",
                "no valid virtual kafka bootstrap")) {
            return true;
        }

        // CC/SOCKS5 offline or unreachable at startup.
        // The tunnel is established once during initialization; if it fails here the
        // Cloud Connector is down, the virtual mapping is missing, or the SAP CC
        // location ID is wrong. None of these will self-heal within the same deployed
        // instance — the operator must fix CC and redeploy. Retrying every 5s just
        // floods CPI logs with the same error and burns resources.
        if (containsAny(l,
                "connection refused",
                "connection reset",
                "connect timed out",
                "read timed out",
                "timed out",
                "timeout",
                "unknownhost",
                "unresolved",
                "name or service not known",
                "no such host",
                "network unreachable",
                "socks",
                "rejected all authentication methods",
                "authentication failed",
                "0xff")) {
            return true;
        }

        // Any other tunnel failure at startup is also treated as nonRetryable.
        // A successful tunnel open followed by a mid-consumption drop is handled
        // separately in the poll() error path with a single reconnect attempt.
        return true;
    }

    private boolean isNonRetryableKafkaStartupFailure(final String cause) {
        final String l = lowerSafe(cause);

        // Authentication and credential errors — wrong password, wrong alias, cert not imported.
        // None of these self-heal without operator action + redeploy.
        if (containsAny(l,
                // SASL
                "sasl authentication failed",
                "authentication failed",
                "invalid credentials",
                "credential alias is empty",
                "credential alias not found",
                // SSL / mTLS handshake
                "ssl handshake failed",
                "ssl handshake exception",
                "handshake_failure",
                // Certificate chain / trust issues — CA not imported, wrong CA alias
                "pkix path building failed",
                "unable to find valid certification path",
                "certificate verify failed",
                "certificate unknown",
                "unknown ca",
                "bad certificate",
                "certificate expired",
                "certificate revoked",
                // Client cert / private key not found in Keystore (alias wrong or not imported)
                "no such alias",
                "keystore",
                "key store",
                "unrecoverable key",
                "key management",
                "no private key",
                "private key",
                // Hostname verification
                "no name matching",
                "hostname in certificate didn't match",
                "hostname verification failed",
                // Authorization
                "topic authorization failed",
                "group authorization failed",
                "cluster authorization failed",
                "token endpoint",
                "invalid_client",
                "unauthorized_client")) {
            return true;
        }

        // Network/broker transient errors — these can self-heal, allow retry.
        if (containsAny(l,
                "timeout",
                "timed out",
                "connection refused",
                "connection reset",
                "broker may not be available",
                "node disconnected",
                "disconnect",
                "unreachable",
                "metadata")) {
            return false;
        }

        // Default for unknown errors at Kafka startup: non-retryable.
        // Better to stop and surface than to spam logs with infinite retries.
        return true;
    }

    private boolean isNonRetryableStartupFailure(final String cause) {
        final String l = lowerSafe(cause);
        if (containsAny(l,
                "invalid adapter channel configuration",
                "fixed schema id",
                "schema id",
                "credential alias is empty",
                "group.id resolved to empty",
                "explicit partitions can only be used",
                "invalid kafka bootstrap entry",
                "invalid kafka bootstrap port",
                // mTLS config errors
                "mtlskeyalias",
                "mtls authentication requires",
                "connectwithtls",
                "token endpoint returned http",
                "token endpoint response did not contain",
                // Generic bad config
                "is not configured",
                "requires 'mtls")) {
            return true;
        }
        return false;
    }

    private String classifyCloudConnectorTunnelCode(final String root) {
        final String l = lowerSafe(root);
        if (containsAny(l, "address already in use", "local port is already in use", "not registered in the current osgi relay registry")) {
            return "Event.Smart.Kafka.TcpRelay.Bind.AddressInUse";
        }
        if (containsAny(l, "read timed out", "timed out", "timeout")) {
            return "Event.Smart.Kafka.TcpRelay.Socks5.ReadTimeout";
        }
        if (containsAny(l, "connection refused")) {
            return "Event.Smart.Kafka.TcpRelay.Connection.Refused";
        }
        if (containsAny(l, "unknownhost", "unresolved", "name or service not known", "no such host")) {
            return "Event.Smart.Kafka.TcpRelay.Host.Resolve.Failed";
        }
        if (containsAny(l, "jwt", "authentication failed", "rejected all authentication methods", "0xff")) {
            return "Event.Smart.Kafka.TcpRelay.Socks5.Auth.Failed";
        }
        return "Event.Smart.Kafka.Config.CloudConnector.Tunnel.Failed";
    }

    private String titleForCloudConnectorTunnelCode(final String code) {
        if ("Event.Smart.Kafka.TcpRelay.Bind.AddressInUse".equals(code)) {
            return "Local TCP relay port already in use";
        }
        if ("Event.Smart.Kafka.TcpRelay.Socks5.ReadTimeout".equals(code)) {
            return "Cloud Connector SOCKS5 handshake timed out";
        }
        if ("Event.Smart.Kafka.TcpRelay.Connection.Refused".equals(code)) {
            return "Cloud Connector TCP proxy refused connection";
        }
        if ("Event.Smart.Kafka.TcpRelay.Host.Resolve.Failed".equals(code)) {
            return "Cloud Connector TCP host could not be resolved";
        }
        if ("Event.Smart.Kafka.TcpRelay.Socks5.Auth.Failed".equals(code)) {
            return "Cloud Connector SOCKS5 authentication failed";
        }
        return "Cloud Connector TCP tunnel failed";
    }

    private String causeForCloudConnectorTunnelCode(final String code) {
        if ("Event.Smart.Kafka.TcpRelay.Bind.AddressInUse".equals(code)) {
            return "The local CPI relay port is already held by another adapter/runtime instance.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Socks5.ReadTimeout".equals(code)) {
            return "The TCP relay reached the SAP Connectivity SOCKS5 path, but the handshake did not answer in time. This startup failure is retryable.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Connection.Refused".equals(code)) {
            return "The SAP Connectivity SOCKS5 proxy or local relay target refused the TCP connection. This startup failure is retryable.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Host.Resolve.Failed".equals(code)) {
            return "The configured Cloud Connector proxy or virtual Kafka host cannot be resolved.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Socks5.Auth.Failed".equals(code)) {
            return "The SAP Connectivity SOCKS5 proxy rejected JWT/authentication or Location ID.";
        }
        return "The adapter could not open the SOCKS5 tunnel through SAP Cloud Connector.";
    }

    private String fixForCloudConnectorTunnelCode(final String code) {
        if ("Event.Smart.Kafka.TcpRelay.Bind.AddressInUse".equals(code)) {
            return "1. Undeploy all iFlows using this relay port.\n"
                    + "2. If the port stays locked, use a fresh relay port or recycle the CPI worker.\n"
                    + "3. Restarting SAP Cloud Connector alone may not release a CPI-side port.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Socks5.ReadTimeout".equals(code)) {
            return "1. Verify SAP Cloud Connector is connected and the TCP mapping is reachable.\n"
                    + "2. Verify virtual host, virtual port and Location ID.\n"
                    + "3. The adapter will retry automatically; redeploy is not required.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Connection.Refused".equals(code)) {
            return "1. Verify the SAP Connectivity SOCKS5 proxy is available.\n"
                    + "2. Verify Cloud Connector and local Kafka listener are up.\n"
                    + "3. The adapter will retry automatically; redeploy is not required.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Host.Resolve.Failed".equals(code)) {
            return "1. Verify Cloud Connector virtual host and bootstrap host spelling.\n"
                    + "2. Verify the SAP BTP Connectivity proxy environment is available.\n"
                    + "3. Redeploy after correcting the channel/runtime configuration.";
        }
        if ("Event.Smart.Kafka.TcpRelay.Socks5.Auth.Failed".equals(code)) {
            return "1. Verify SAP Connectivity JWT is available to the CPI runtime.\n"
                    + "2. Verify Location ID only when the Cloud Connector uses one.\n"
                    + "3. Redeploy after correcting the connectivity configuration.";
        }
        return "1. Verify Cloud Connector TCP virtual host and port mapping.\n"
                + "2. Verify Location ID when a named Cloud Connector is used.\n"
                + "3. Verify Kafka advertised.listeners routes back to the local relay port.";
    }

    private String buildConsumerGroupMissingError() {
        return buildCompactRuntimeError(
                "Kafka consumer group ID is missing",
                "Event.Smart.Kafka.Config.GroupId.Missing",
                commonKafkaContext(endpoint.resolveBootstrapServers()),
                "Kafka cannot persist offsets without a stable group.id.",
                "1. Set a stable Group ID in the adapter channel.\n"
                        + "2. Do not generate a random group ID per deployment.\n"
                        + "3. Redeploy the iFlow.",
                "group.id resolved to empty/null.");
    }

    private String buildPartitionConfigurationError(final String topicExpression, final String cause) {
        return buildCompactRuntimeError(
                "Explicit partition configuration is invalid",
                "Event.Smart.Kafka.Config.Partitions.Invalid",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nTopics     : " + safeValue(topicExpression)
                        + "\nPartitions : " + safeValue(endpoint.getPartitions()),
                "Explicit partitions can only be used with one exact topic.",
                "1. Use one exact topic when Partitions is filled.\n"
                        + "2. Leave Partitions empty when using wildcard or multi-topic subscription.\n"
                        + "3. Redeploy after correcting the channel.",
                safeRoot(cause));
    }

    private String buildMissingTopicError(final List<String> missingTopics) {
        return buildCompactRuntimeError(
                "Kafka topic does not exist",
                "Event.Smart.Kafka.Config.Topic.NotFound",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nMissing    : " + String.valueOf(missingTopics),
                "One or more configured topics were not found in the target Kafka cluster.",
                "1. Create the missing topic or enable controlled auto-create.\n"
                        + "2. Check spelling, namespace and case.\n"
                        + "3. Verify the adapter is connected to the expected broker environment.",
                "Missing topic(s): " + String.valueOf(missingTopics));
    }

    private String buildUnexpectedStartupError(final String cause) {
        return buildCompactRuntimeError(
                "Unexpected startup failure",
                "Event.Smart.Kafka.Startup.Unexpected.Failure",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nSecurity   : " + describeSecurityProfile(),
                "The adapter failed during startup before normal polling could begin.",
                "1. Review the root cause below.\n"
                        + "2. Check recent channel, certificate or broker topology changes.\n"
                        + "3. Redeploy after correcting the startup dependency.",
                safeRoot(cause));
    }

    private String buildNoCommittedOffsetError(final String cause) {
        return buildCompactRuntimeError(
                "No committed offset for auto.offset.reset=NONE",
                "Event.Smart.Kafka.Offset.NoCommittedOffset",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nGroup      : " + safeValue(effectiveGroupId)
                        + "\nOffsetMode : " + safeValue(endpoint.getAutoOffsetReset()),
                "The consumer group has no committed offset and reset policy is NONE.",
                "1. Use EARLIEST to start from the oldest retained record.\n"
                        + "2. Use LATEST to wait for new records only.\n"
                        + "3. Keep NONE only when the group already has committed offsets.",
                safeRoot(cause));
    }

    private String buildPollFailureError(final String cause) {
        return buildCompactRuntimeError(
                "Kafka poll failed",
                "Event.Smart.Kafka.Engine.Poll.Failure",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nGroup      : " + safeValue(effectiveGroupId),
                "Kafka poll failed and the consumer will be recycled.",
                "1. Check broker availability and consumer group stability.\n"
                        + "2. Check max.poll.interval.ms, session timeout and broker ACLs.\n"
                        + "3. If On-Premise, verify the Cloud Connector tunnel is still active.",
                safeRoot(cause));
    }

    private String buildPipelineProcessorError(final ConsumerRecord<String, byte[]> record, final String cause) {
        final String seekHint = record != null && record.timestamp() >= 0
                ? "Use TIMESEEK with EpochMs=" + formatKafkaRecordEpochMs(record.timestamp())
                  + " (ISO: " + formatKafkaRecordTimeUtc(record.timestamp()) + ") to recover this record."
                : null;

        final String fix = "1. Check the iFlow step that failed after the Sender channel.\n"
                + "2. Check mapping, content modifier, script or receiver call.\n"
                + (seekHint != null ? "3. " + seekHint
                : "3. Enable Message Recovery TIMESEEK to replay this record.");

        return buildCompactRuntimeError(
                "CPI pipeline processing failed",
                "Event.Smart.Kafka.Record.Processing.Failure",
                recordContext(record),
                "The Kafka record reached the iFlow but a downstream processing step failed.",
                fix,
                safeRoot(cause));
    }

    private String buildRecoveryRestoreError(final String cause) {
        return buildCompactRuntimeError(
                "TIMESEEK offset restore failed",
                "Event.Smart.Kafka.Recovery.OffsetRestore.Failed",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nGroup      : " + safeValue(effectiveGroupId)
                        + "\nSeekInput  : " + safeValue(recoverySeekInput)
                        + "\nSeekMs     : " + recoverySeekTimestampMs
                        + "\nOriginal   : " + formatOffsets(recoveryOriginalCommittedOffsets),
                "The recovered record was processed, but the original committed offset could not be restored.",
                "1. Do not disable Message Recovery yet.\n"
                        + "2. Verify consumer group commit permissions.\n"
                        + "3. Retry or manually reset the group offset in Kafka tooling.",
                safeRoot(cause));
    }

    private String buildRecoveryPartitionResolutionError(final long seekTs, final String partitionsParam) {
        return buildCompactRuntimeError(
                "TIMESEEK could not resolve partitions",
                "Event.Smart.Kafka.Recovery.Partitions.NotResolved",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nSeekInput  : " + safeValue(recoverySeekInput)
                        + "\nSeekMs     : " + seekTs
                        + "\nPartitions : " + safeValue(partitionsParam),
                "The recovery seek could not resolve Kafka partitions for the configured topic expression.",
                "1. Verify the topic exists.\n"
                        + "2. If using wildcard, verify it matches existing topics.\n"
                        + "3. If using explicit partitions, verify partition numbers exist.",
                "No partitions resolved for TIMESEEK.");
    }

    private String buildRecoveryNoOffsetError(final long seekTs, final String noOffsetPartitions) {
        return buildCompactRuntimeError(
                "TIMESEEK found no offset for timestamp",
                "Event.Smart.Kafka.Recovery.Timestamp.NoOffset",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nSeekInput  : " + safeValue(recoverySeekInput)
                        + "\nSeekMs     : " + seekTs
                        + "\nNoOffset   : " + safeValue(noOffsetPartitions),
                "No record offset exists at or after the requested timestamp within the topic retention window.",
                "1. Use a timestamp inside the topic retention window.\n"
                        + "2. Try one or two seconds earlier.\n"
                        + "3. Confirm whether the topic uses CreateTime or LogAppendTime.",
                "offsetsForTimes returned no offset for the requested timestamp.");
    }

    private String buildRecoverySeekFailedError(final long seekTs, final String cause) {
        return buildCompactRuntimeError(
                "TIMESEEK failed",
                "Event.Smart.Kafka.Recovery.Seek.Failed",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nSeekInput  : " + safeValue(recoverySeekInput)
                        + "\nSeekMs     : " + seekTs,
                "The adapter could not seek the consumer to the requested timestamp.",
                "1. Verify timestamp format and value.\n"
                        + "2. Verify topic, partitions and permissions.\n"
                        + "3. Disable Message Recovery if normal consumption is required.",
                safeRoot(cause));
    }

    private String buildOffsetCommitFailureError(final String offsets, final String cause) {
        return buildCompactRuntimeError(
                "Kafka offset commit failed",
                "Event.Smart.Kafka.Offset.Commit.Failed",
                commonKafkaContext(endpoint.resolveBootstrapServers())
                        + "\nGroup      : " + safeValue(effectiveGroupId)
                        + "\nOffsets    : " + safeValue(offsets),
                "The record was processed, but Kafka offset commit failed.",
                "1. Verify consumer group ACLs and commit permissions.\n"
                        + "2. Check group.id stability, session timeout and max.poll.interval.ms.\n"
                        + "3. The consumer will recycle and retry according to runtime policy.",
                safeRoot(cause));
    }

    // Overload com record — exibe timestamp para facilitar TIMESEEK recovery
    private String buildOffsetCommitFailureError(final ConsumerRecord<String, byte[]> record,
                                                 final String offsets,
                                                 final String cause) {
        if (record == null) {
            return buildOffsetCommitFailureError(offsets, cause);
        }
        final String seekHint = record.timestamp() >= 0
                ? "Use TIMESEEK EpochMs=" + formatKafkaRecordEpochMs(record.timestamp())
                  + " (ISO: " + formatKafkaRecordTimeUtc(record.timestamp()) + ") to recover this record."
                : null;
        return buildCompactRuntimeError(
                "Kafka offset commit failed",
                "Event.Smart.Kafka.Offset.Commit.Failed",
                recordContext(record)
                        + "\nOffsets    : " + safeValue(offsets),
                "The record was processed, but Kafka offset commit failed.",
                "1. Verify consumer group ACLs and commit permissions.\n"
                        + "2. Check group.id stability, session timeout and max.poll.interval.ms.\n"
                        + (seekHint != null ? "3. " + seekHint
                        : "3. Use Message Recovery TIMESEEK to replay this record."),
                safeRoot(cause));
    }

    private String buildAvroPoisonCommitFailureError(final ConsumerRecord<String, byte[]> record, final String cause) {
        return buildCompactRuntimeError(
                "Avro poison offset commit failed",
                "Event.Smart.Kafka.Avro.PoisonCommit.Failed",
                recordContext(record)
                        + "\nGroup      : " + safeValue(effectiveGroupId)
                        + "\nCommit     : " + (record == null ? "<unknown>" : String.valueOf(record.offset() + 1)),
                "The Avro poison record failed, and its skip offset could not be committed.",
                "1. Verify consumer group commit ACLs.\n"
                        + "2. Verify group.id stability.\n"
                        + "3. The same poison record will be retried until commit succeeds or policy changes.",
                safeRoot(cause));
    }

    private String classifyBrokerStartupCode(final String cause) {
        final String d = cause == null ? "" : cause.toLowerCase(java.util.Locale.ROOT);
        if (d.indexOf("sasl") >= 0 || d.indexOf("authentication failed") >= 0
                || d.indexOf("invalid username") >= 0 || d.indexOf("invalid password") >= 0
                || d.indexOf("login") >= 0 || d.indexOf("credential") >= 0) {
            return "Event.Smart.Kafka.Auth.Failed";
        }
        if (d.indexOf("ssl") >= 0 || d.indexOf("pkix") >= 0 || d.indexOf("certificate") >= 0
                || d.indexOf("handshake") >= 0 || d.indexOf("trustanchor") >= 0
                || d.indexOf("subject alternative") >= 0) {
            return "Event.Smart.Kafka.TLS.Handshake.Failed";
        }
        if (d.indexOf("no resolvable bootstrap") >= 0 || d.indexOf("unknownhost") >= 0
                || d.indexOf("couldn't resolve") >= 0 || d.indexOf("failed to resolve") >= 0) {
            return "Event.Smart.Kafka.Config.Broker.Resolve.Failed";
        }
        if (d.indexOf("timeout") >= 0 || d.indexOf("timed out") >= 0 || d.indexOf("metadata") >= 0) {
            return "Event.Smart.Kafka.Config.Broker.Metadata.Timeout";
        }
        return "Event.Smart.Kafka.Config.Broker.Connection.Failed";
    }

    private String titleForCode(final String code) {
        if ("Event.Smart.Kafka.Auth.Failed".equals(code)) return "Kafka authentication failed";
        if ("Event.Smart.Kafka.TLS.Handshake.Failed".equals(code)) return "Kafka TLS handshake failed";
        if ("Event.Smart.Kafka.Config.Broker.Resolve.Failed".equals(code)) return "Kafka bootstrap address could not be resolved";
        if ("Event.Smart.Kafka.Config.Broker.Metadata.Timeout".equals(code)) return "Kafka metadata lookup timed out";
        return "Kafka broker connection failed";
    }

    private String causeForCode(final String code) {
        if ("Event.Smart.Kafka.Auth.Failed".equals(code)) {
            return "Kafka rejected the configured SASL credentials or mechanism.";
        }
        if ("Event.Smart.Kafka.TLS.Handshake.Failed".equals(code)) {
            return "Kafka TLS handshake failed because trust, certificate chain or hostname verification is invalid.";
        }
        if ("Event.Smart.Kafka.Config.Broker.Resolve.Failed".equals(code)) {
            return "Kafka could not resolve a usable bootstrap broker address.";
        }
        if ("Event.Smart.Kafka.Config.Broker.Metadata.Timeout".equals(code)) {
            return "Kafka connected far enough to attempt metadata lookup, but broker metadata was not returned in time.";
        }
        return "Kafka could not connect to the configured bootstrap broker.";
    }

    private String fixForCode(final String code, final String root) {
        if ("Event.Smart.Kafka.Auth.Failed".equals(code)) {
            return "1. Verify CPI User Credentials alias, username and password.\n"
                    + "2. Verify SASL mechanism matches the broker listener.\n"
                    + "3. Verify the broker user exists and has topic/group ACLs.";
        }
        if ("Event.Smart.Kafka.TLS.Handshake.Failed".equals(code)) {
            return "1. Verify the selected trust source contains the broker CA.\n"
                    + "2. Verify certificate SAN matches the adapter hostname.\n"
                    + "3. For local relay tests, include 127.0.0.1 or the virtual host in SAN.";
        }
        if ("Event.Smart.Kafka.Config.Broker.Resolve.Failed".equals(code)) {
            return "1. Check bootstrap hostname and port for typos.\n"
                    + "2. If On-Premise, verify Cloud Connector TCP virtual host and port.\n"
                    + "3. Verify advertised.listeners returns a reachable relay address.";
        }
        if ("Event.Smart.Kafka.Config.Broker.Metadata.Timeout".equals(code)) {
            return "1. Verify the broker is reachable from the selected network path.\n"
                    + "2. If On-Premise, verify Cloud Connector mapping and tunnel startup.\n"
                    + "3. Verify advertised.listeners does not return an internal-only hostname.";
        }
        return "1. Verify bootstrap hostname and port.\n"
                + "2. If On-Premise, verify Cloud Connector TCP mapping and Location ID.\n"
                + "3. Verify broker advertised.listeners returns a reachable address.";
    }

    private String lowerSafe(final String value) {
        return value == null ? "" : value.toLowerCase(java.util.Locale.ROOT);
    }

    private boolean containsAny(final String value, final String... needles) {
        if (value == null || needles == null) {
            return false;
        }
        for (int i = 0; i < needles.length; i++) {
            final String n = needles[i];
            if (n != null && n.length() > 0 && value.indexOf(n) >= 0) {
                return true;
            }
        }
        return false;
    }

    private String describeSecurityProfile() {
        final String authentication = endpoint.getAuthentication();
        final boolean mtls = endpoint.isMtlsAuthentication(authentication);
        final boolean tls  = mtls || Boolean.TRUE.equals(endpoint.getConnectWithTls());
        if (mtls) {
            return "mTLS / SSL";
        }
        if (isPlainTextAuthentication(authentication)) {
            return tls ? "NONE / SSL" : "NONE / PLAINTEXT";
        }
        return "SASL / " + safeValue(endpoint.getSaslMechanism()) + (tls ? " / TLS" : " / no TLS");
    }

    private String describeCredentialAlias() {
        final String authentication = endpoint.getAuthentication();
        if (isPlainTextAuthentication(authentication)) {
            return "n/a";
        }
        if (endpoint.isMtlsAuthentication(authentication)) {
            return safeValue(endpoint.getMtlsKeyAlias());
        }
        return safeValue(endpoint.getCredentialAlias());
    }

    private String commonKafkaContext(final String bootstrapServers) {
        return "Bootstrap : " + safeValue(bootstrapServers)
                + "\nTopic     : " + safeValue(endpoint.getTopicPattern())
                + "\nProxy     : " + safeValue(endpoint.getProxyType());
    }

    private String recordContext(final ConsumerRecord<String, byte[]> record) {
        if (record == null) {
            return commonKafkaContext(endpoint.resolveBootstrapServers())
                    + "\nPartition : <unknown>"
                    + "\nOffset    : <unknown>"
                    + "\nTimestamp : <unknown>";
        }
        return "Topic     : " + safeValue(record.topic())
                + "\nPartition : " + record.partition()
                + "\nOffset    : " + record.offset()
                + "\nTimestamp : " + formatKafkaRecordTimeUtc(record.timestamp())
                + "\nEpochMs   : " + formatKafkaRecordEpochMs(record.timestamp())
                + "\nBootstrap : " + safeValue(endpoint.resolveBootstrapServers())
                + "\nProxy     : " + safeValue(endpoint.getProxyType())
                + "\nGroup     : " + safeValue(effectiveGroupId);
    }

    private String formatKafkaRecordTimeUtc(final long timestampMs) {
        if (timestampMs < 0L) {
            return "<unavailable>";
        }
        try {
            return java.time.Instant.ofEpochMilli(timestampMs).toString();
        } catch (Throwable ignored) {
            return "<invalid>";
        }
    }

    private String formatKafkaRecordEpochMs(final long timestampMs) {
        return timestampMs < 0L ? "<unavailable>" : String.valueOf(timestampMs);
    }

    private String buildCompactRuntimeError(final String title,
                                            final String code,
                                            final String context,
                                            final String cause,
                                            final String fix,
                                            final String rootCause) {
        final StringBuilder err = new StringBuilder(768);
        // Leading blank line is intentional: CPI renders this after
        // "org.apache.camel.RuntimeCamelException:" in Last Error.
        err.append("\n\n⛔ SDIA - EventSmartKafka — ").append(safeLine(title, 105));
        err.append("\n\nCode      : ").append(safeValue(code));
        appendContextBlock(err, context);
        // No separate "Cause:" block by design — the short human explanation that used to live
        // here is folded into Root cause below when it adds information the raw detail doesn't
        // already convey, so the format stays Error -> Root cause -> Fix with no duplication.
        err.append("\n\n🔎 Root cause:\n\n").append(safeLine(mergeCauseIntoRootCause(cause, rootCause), 220));
        appendFixBlock(err, fix);
        return err.toString();
    }

    /**
     * Folds the short human-readable "cause" explanation into the technical root cause detail,
     * without duplicating text when one already contains the other. Keeps every existing caller
     * of buildCompactRuntimeError working unchanged while removing the separate "Cause:" block
     * from the rendered Last Error.
     */
    private String mergeCauseIntoRootCause(final String cause, final String rootCause) {
        final String c = trimToNull(cause);
        final String r = trimToNull(compactRootCause(rootCause));
        if (c == null) return r == null ? "Unknown error." : r;
        if (r == null) return c;
        final String cLower = c.toLowerCase(java.util.Locale.ROOT);
        final String rLower = r.toLowerCase(java.util.Locale.ROOT);
        if (rLower.indexOf(cLower) >= 0 || cLower.indexOf(rLower) >= 0) {
            // One already contains the other's meaning — keep only the more specific one
            // (the longer string is assumed to carry more detail).
            return r.length() >= c.length() ? r : c;
        }
        return c + " " + r;
    }

    private void appendContextBlock(final StringBuilder err, final String context) {
        if (context == null || context.trim().isEmpty()) {
            return;
        }
        final String[] lines = context.split("\\r?\\n");
        int added = 0;
        for (int i = 0; i < lines.length && added < 10; i++) {
            String line = normalizeDiagnosticLine(lines[i]);
            if (line.length() == 0) continue;
            final String lower = line.toLowerCase(java.util.Locale.ROOT);
            if (lower.startsWith("error code:") || lower.startsWith("error detail:") || lower.startsWith("detail:")) {
                continue;
            }
            err.append("\n").append(safeLine(line, 135));
            added++;
        }
    }

    private void appendFixBlock(final StringBuilder err, final String fix) {
        err.append("\n\n🛠️ Fix:\n");
        if (fix == null || fix.trim().isEmpty()) {
            err.append("\n1. Review the root cause and correct the adapter channel configuration.");
            return;
        }
        final String[] lines = normalizeFixText(fix).split("\\r?\\n");
        int added = 0;
        for (int i = 0; i < lines.length && added < 3; i++) {
            String line = normalizeDiagnosticLine(lines[i]);
            if (line.length() == 0) continue;
            line = stripNumberPrefix(line);
            err.append("\n").append(added + 1).append(". ").append(safeLine(line, 125));
            added++;
        }
        if (added == 0) {
            err.append("\n1. Review the root cause and correct the adapter channel configuration.");
        }
    }

    private String normalizeFixText(final String fix) {
        if (fix == null) return "";
        String out = fix.replace("\r", "\n");
        // Some old messages were flattened into one line: "... 2 - ... 3 - ...".
        out = out.replace(" 2 - ", "\n2 - ").replace(" 3 - ", "\n3 - ").replace(" 4 - ", "\n4 - ");
        out = out.replace(" 2. ", "\n2. ").replace(" 3. ", "\n3. ").replace(" 4. ", "\n4. ");
        return out;
    }

    private String buildVisualError(String title, String detail, String correction) {
        if (isVisualAdapterError(detail)) {
            return extractBestVisualAdapterError(detail);
        }
        final String code = firstNonEmpty(extractDiagnosticValue(detail, "Error Code:"),
                inferCodeFromTitle(title));
        final String root = firstNonEmpty(extractDiagnosticValue(detail, "Error detail:"),
                extractDiagnosticValue(detail, "Detail:"),
                extractDiagnosticValue(detail, "Error:"),
                safeRoot(detail));
        final String context = compactContext(detail);
        return buildCompactRuntimeError(
                cleanTitle(title),
                code,
                context,
                summarizeCause(cleanTitle(title), root),
                correction,
                root);
    }

    private String compactContext(final String detail) {
        if (detail == null) return "";
        final StringBuilder out = new StringBuilder(256);
        final String[] lines = detail.split("\\r?\\n");
        int added = 0;
        for (int i = 0; i < lines.length && added < 8; i++) {
            String line = normalizeDiagnosticLine(lines[i]);
            if (line.length() == 0) continue;
            final String lower = line.toLowerCase(java.util.Locale.ROOT);
            if (lower.startsWith("error detail:") || lower.startsWith("error code:")
                    || lower.startsWith("detail:") || lower.startsWith("error:")) {
                continue;
            }
            if (out.length() > 0) out.append('\n');
            out.append(line);
            added++;
        }
        return out.toString();
    }

    private String extractDiagnosticValue(final String text, final String marker) {
        if (text == null || marker == null) return null;
        final String[] lines = text.split("\\r?\\n");
        for (int i = 0; i < lines.length; i++) {
            String line = normalizeDiagnosticLine(lines[i]);
            final int idx = line.toLowerCase(java.util.Locale.ROOT).indexOf(marker.toLowerCase(java.util.Locale.ROOT));
            if (idx >= 0) {
                String value = line.substring(idx + marker.length()).trim();
                return value.length() == 0 ? null : value;
            }
        }
        return null;
    }

    private String normalizeDiagnosticLine(final String line) {
        if (line == null) return "";
        String v = line.trim();
        boolean changed = true;
        while (changed && v.length() > 0) {
            changed = false;
            if (v.startsWith("➔")) {
                v = v.substring("➔".length()).trim();
                changed = true;
            } else if (v.startsWith("🔴")) {
                v = v.substring("🔴".length()).trim();
                changed = true;
            } else if (v.startsWith("⚠️")) {
                v = v.substring("⚠️".length()).trim();
                changed = true;
            } else if (v.startsWith("⚠")) {
                v = v.substring("⚠".length()).trim();
                changed = true;
            } else if (v.startsWith("-")) {
                v = v.substring(1).trim();
                changed = true;
            }
        }
        return v;
    }

    private String stripNumberPrefix(final String line) {
        if (line == null) return "";
        String v = line.trim();
        int i = 0;
        while (i < v.length() && Character.isDigit(v.charAt(i))) i++;
        if (i > 0 && i < v.length()) {
            char c = v.charAt(i);
            if (c == '-' || c == '.' || c == ')') {
                return v.substring(i + 1).trim();
            }
        }
        return v;
    }

    private String cleanTitle(final String title) {
        if (title == null || title.trim().isEmpty()) return "Adapter failure";
        String t = title.trim();
        t = t.replace("Adapter Configuration Failure - ", "");
        t = t.replace("Event Smart Kafka — ", "");
        if (t.endsWith(".")) t = t.substring(0, t.length() - 1);
        return t;
    }

    private String summarizeCause(final String title, final String root) {
        final String t = title == null ? "" : title.toLowerCase(java.util.Locale.ROOT);
        if (t.indexOf("credential") >= 0) return "The configured credential is missing, invalid or cannot be loaded.";
        if (t.indexOf("tls") >= 0 || t.indexOf("certificate") >= 0) return "TLS trust or hostname verification failed.";
        if (t.indexOf("topic") >= 0) return "The configured topic is not available in the connected Kafka cluster.";
        if (t.indexOf("offset") >= 0) return "Kafka offset positioning or commit failed.";
        if (t.indexOf("recovery") >= 0 || t.indexOf("timeseek") >= 0) return "Message Recovery could not complete the requested seek operation.";
        if (t.indexOf("avro") >= 0) return "The Kafka record could not be decoded with the selected Avro schema.";
        if (t.indexOf("pipeline") >= 0 || t.indexOf("processor") >= 0) return "The record reached CPI, but downstream iFlow processing failed.";
        if (t.indexOf("connector") >= 0 || t.indexOf("tunnel") >= 0) return "The SAP Cloud Connector TCP/SOCKS5 path could not be established.";
        if (root != null && root.trim().length() > 0) return safeLine(root, 220);
        return "The adapter failed while processing the configured Kafka channel.";
    }

    private String inferCodeFromTitle(final String title) {
        final String t = title == null ? "" : title.toLowerCase(java.util.Locale.ROOT);
        if (t.indexOf("credential") >= 0) return "Event.Smart.Kafka.Config.Credential.Failed";
        if (t.indexOf("bootstrap") >= 0) return "Event.Smart.Kafka.Config.Bootstrap.Failed";
        if (t.indexOf("topic") >= 0) return "Event.Smart.Kafka.Config.Topic.Failed";
        if (t.indexOf("offset commit") >= 0) return "Event.Smart.Kafka.Offset.Commit.Failed";
        if (t.indexOf("offset") >= 0) return "Event.Smart.Kafka.Offset.Failed";
        if (t.indexOf("recovery") >= 0) return "Event.Smart.Kafka.Recovery.Failed";
        if (t.indexOf("avro") >= 0) return "Event.Smart.Kafka.Avro.Failed";
        if (t.indexOf("pipeline") >= 0 || t.indexOf("processor") >= 0) return "Event.Smart.Kafka.Record.Processing.Failure";
        if (t.indexOf("connector") >= 0 || t.indexOf("tunnel") >= 0) return "Event.Smart.Kafka.Config.CloudConnector.Tunnel.Failed";
        return "Event.Smart.Kafka.Adapter.Failure";
    }

    private String firstNonEmpty(final String a, final String b) {
        return a != null && a.trim().length() > 0 ? a.trim() : b;
    }

    private String firstNonEmpty(final String a, final String b, final String c) {
        return firstNonEmpty(firstNonEmpty(a, b), c);
    }

    private String firstNonEmpty(final String a, final String b, final String c, final String d) {
        return firstNonEmpty(firstNonEmpty(a, b, c), d);
    }

    private String safeRoot(final String value) {
        if (value == null || value.trim().isEmpty()) return "Unknown root cause";
        return value.trim();
    }

    private String safeValue(final Object value) {
        if (value == null) return "<empty>";
        final String s = String.valueOf(value).trim();
        return s.length() == 0 ? "<empty>" : s;
    }

    private String safeLine(final Object value, final int maxLen) {
        String s = safeValue(value).replace('\r', ' ').replace('\n', ' ').trim();
        while (s.indexOf("  ") >= 0) s = s.replace("  ", " ");
        if (maxLen > 0 && s.length() > maxLen) {
            return s.substring(0, Math.max(0, maxLen - 3)) + "...";
        }
        return s;
    }

    private String oneLineForLog(final String message) {
        return safeLine(message, 240);
    }

    private String compactRootCause(final String rootCause) {
        if (rootCause == null) return "Unknown root cause";
        String r = rootCause.trim();
        final String lower = r.toLowerCase(java.util.Locale.ROOT);
        if (lower.indexOf("payload size") >= 0 && lower.indexOf("conversion limit") >= 0) {
            final String size = extractBetweenLoose(r, "Payload size", "bytes");
            final String limit = extractBetweenLoose(r, "limit of", "MB");
            if (size != null && limit != null) {
                return "Payload " + size.trim() + " bytes exceeds Avro conversion limit " + limit.trim() + " MB.";
            }
            return "Payload exceeds the configured Avro conversion limit.";
        }
        final String camelCause = ", cause: org.apache.camel.RuntimeCamelException:";
        final int idx = r.indexOf(camelCause);
        if (idx >= 0) r = r.substring(0, idx).trim();
        return r;
    }

    private String extractBetweenLoose(final String value, final String left, final String right) {
        if (value == null) return null;
        final int l = value.indexOf(left);
        if (l < 0) return null;
        final int start = l + left.length();
        final int end = value.indexOf(right, start);
        if (end < 0) return null;
        final String out = value.substring(start, end).trim();
        return out.length() == 0 ? null : out;
    }

    private String stripSdiaPrefix(final String value) {
        if (value == null) {
            return "";
        }
        String v = value.trim();
        final String prefix = "[SDIA Kafka]";
        if (v.startsWith(prefix)) {
            v = v.substring(prefix.length()).trim();
        }
        return v;
    }

    private String extractAfter(final String value, final String marker) {
        if (value == null || marker == null) {
            return null;
        }
        final int idx = value.toLowerCase().indexOf(marker.toLowerCase());
        if (idx < 0) {
            return null;
        }
        String out = value.substring(idx + marker.length()).trim();
        if (out.endsWith(".")) {
            out = out.substring(0, out.length() - 1).trim();
        }
        return out.length() == 0 ? null : out;
    }

    private String buildCompactAvroError(final ConsumerRecord<String, byte[]> record,
                                         final String errCode,
                                         final String schemaMode,
                                         final Object schemaId,
                                         final String detail,
                                         final boolean poisonOffsetCommitted) {
        final String cleanDetail = cleanAvroErrorDetail(detail);
        final String correction = buildAvroCorrection(errCode, schemaMode, schemaId, cleanDetail);
        final String title = buildAvroErrorTitle(errCode, cleanDetail, poisonOffsetCommitted);
        final String context = recordContext(record)
                + "\nSchemaMode: " + safeValue(schemaMode)
                + "\nSchemaId  : " + safeValue(schemaId)
                + "\nHandling  : " + (poisonOffsetCommitted ? "Skip failed record" : "Retry/Stop policy");
        return buildCompactRuntimeError(
                title,
                safeValue(errCode),
                context,
                summarizeAvroCause(errCode, cleanDetail),
                correction,
                cleanDetail);
    }

    private String buildAvroErrorTitle(final String errCode,
                                       final String detail,
                                       final boolean poisonOffsetCommitted) {
        final String code = errCode == null ? "" : errCode;
        final String d = detail == null ? "" : detail.toLowerCase(java.util.Locale.ROOT);
        final String suffix = poisonOffsetCommitted ? "; poison offset committed" : "; offset not committed";

        if ("Event.Smart.Kafka.Payload.TooLarge".equalsIgnoreCase(code)
                || d.indexOf("exceeds the configured avro conversion limit") >= 0
                || (d.indexOf("payload size") >= 0 && d.indexOf("conversion limit") >= 0)) {
            return "Avro payload exceeds conversion limit" + suffix;
        }
        if ("Event.Smart.Kafka.Avro.Empty.Conversion.Result".equalsIgnoreCase(code)
                || d.indexOf("empty body") >= 0 || d.indexOf("empty conversion") >= 0) {
            return "Avro conversion returned empty body" + suffix;
        }
        if ("Event.Smart.Kafka.Avro.Magic.SchemaID.Mismatch".equalsIgnoreCase(code)
                || d.indexOf("magic byte payload schema id mismatch") >= 0
                || d.indexOf("wire header contains schema id") >= 0) {
            return "Magic Byte Schema ID mismatch" + suffix;
        }
        if ("Event.Smart.Kafka.Config.Missing.FixedSchemaID".equalsIgnoreCase(code)
                || d.indexOf("fixed schema id is required") >= 0) {
            return "Fixed Schema ID missing" + suffix;
        }
        return poisonOffsetCommitted
                ? "Avro conversion failed; poison offset committed"
                : "Avro conversion failed; offset not committed";
    }

    private String summarizeAvroCause(final String errCode, final String detail) {
        final String code = errCode == null ? "" : errCode;
        final String d = detail == null ? "" : detail.toLowerCase(java.util.Locale.ROOT);
        if ("Event.Smart.Kafka.Payload.TooLarge".equalsIgnoreCase(code)
                || d.indexOf("exceeds the configured avro conversion limit") >= 0) {
            return "The record is larger than this channel's Avro conversion limit.";
        }
        if (d.indexOf("missing confluent avro magic byte") >= 0) {
            return "The channel expects Confluent wire format, but the record does not start with magic byte 0x00.";
        }
        if (d.indexOf("magic byte payload schema id mismatch") >= 0
                || d.indexOf("wire header contains schema id") >= 0) {
            return "The Magic Byte payload carries a Schema ID different from the expected Schema ID configured in the channel.";
        }
        if (d.indexOf("fixed schema id mismatch") >= 0) {
            return "The record schema ID does not match the Fixed Schema ID configured in the channel.";
        }
        if (d.indexOf("empty body") >= 0 || d.indexOf("empty conversion") >= 0) {
            return "Avro conversion returned an empty body, so the adapter blocked the message instead of passing a blank payload.";
        }
        if (d.indexOf("cannot find 'schema' field") >= 0 || d.indexOf("schema registry") >= 0) {
            return "Schema Registry did not return a valid schema response for the resolved schema ID.";
        }
        if (d.indexOf("invalid avro") >= 0 || d.indexOf("payload ended unexpectedly") >= 0) {
            return "The payload bytes are not valid Avro for the schema selected by the adapter.";
        }
        if (d.indexOf("base64") >= 0) {
            return "The Kafka record value appears to be Base64 text instead of binary Avro bytes.";
        }
        return "The Kafka record could not be decoded using the selected Avro schema.";
    }

    private String cleanAvroErrorDetail(String detail) {
        if (detail == null) {
            return "Unknown exception root cause";
        }
        String d = detail.trim();
        d = compactRootCause(d);
        final String camelCause = ", cause: org.apache.camel.RuntimeCamelException: ";
        final int idx = d.indexOf(camelCause);
        if (idx >= 0) {
            final String before = d.substring(0, idx).trim();
            final String after = d.substring(idx + camelCause.length()).trim();
            if (before.equals(after)) {
                return before;
            }
        }
        return d;
    }

    private String buildAvroCorrection(final String errCode,
                                       final String schemaMode,
                                       final Object schemaId,
                                       final String detail) {
        final String mode = schemaMode == null ? "" : schemaMode;
        final String d = detail == null ? "" : detail.toLowerCase();

        if ("Event.Smart.Kafka.Payload.TooLarge".equalsIgnoreCase(errCode)
                || d.indexOf("exceeds the configured avro conversion limit") >= 0
                || (d.indexOf("payload") >= 0 && d.indexOf("conversion limit") >= 0)) {
            return "Increase Max Payload Conversion Size, or route the 2 MB topic to the correct 2 MB iFlow."
                    + "\nVerify that this channel's Schema ID belongs to this exact payload type."
                    + "\nRecover/replay the record using Message Recovery TIMESEEK with the UnixEpochMs shown above.";
        }

        if ("Event.Smart.Kafka.Invalid.SchemaID.Format".equalsIgnoreCase(errCode)
                || d.indexOf("fixed schema id must be") >= 0
                || d.indexOf("schemaregistryschemaid") >= 0) {
            return "\n1 - Fixed Schema ID must be a positive numeric Schema Registry ID."
                    + "\n2 - Do not use a subject name such as order.created-value or aaaa-value in the Schema ID field."
                    + "\n3 - Register the schema first, copy the numeric id returned by Schema Registry, then redeploy the iFlow."
                    + "\n4 - If the payload has a Confluent Magic Wire header, switch Schema ID Source to Magic Byte instead of Fixed Schema ID.";
        }

        if (d.indexOf("magic byte payload schema id mismatch") >= 0
                || d.indexOf("wire header contains schema id") >= 0) {
            return "\n1 - The payload is Magic Byte / Confluent wire format, but its header Schema ID is not the Schema ID expected by this channel."
                    + "\n2 - Verify the producer used the correct schema for this iFlow, for example do not send the 2 MB schema/payload to a channel expecting the 1 MB schema."
                    + "\n3 - If the channel should accept any Schema ID from the record header, clear schemaRegistrySchemaId or use a dedicated iFlow per schema.";
        }

        if (d.indexOf("empty body") >= 0 || d.indexOf("empty conversion") >= 0) {
            return "\n1 - The adapter produced an empty converted body and blocked it intentionally."
                    + "\n2 - Verify the Fixed Schema ID or Magic Byte header really belongs to this exact payload type and size."
                    + "\n3 - Reproduce with the correct .bin payload and the matching numeric Schema Registry ID.";
        }

        if (d.indexOf("fixed schema id mismatch") >= 0) {
            return "\n1 - The Kafka record contains a Confluent wire header with a different Schema ID than the channel Fixed Schema ID."
                    + "\n2 - Either change the channel to Schema ID Source = Magic Byte, or produce the payload with the same Schema ID configured in the channel."
                    + "\n3 - In Fixed Schema ID mode, a payload with header 100001 must not be accepted by a channel fixed to 100007.";
        }

        if (d.indexOf("missing confluent avro magic byte") >= 0) {
            return "\n1 - The channel is reading the payload as Confluent Magic Wire, but the record does not start with 0x00."
                    + "\n2 - Produce a real Confluent binary payload: 0x00 + 4-byte Schema ID + Avro binary body."
                    + "\n3 - If the payload is raw Avro without the Confluent header, change Schema ID Source to Fixed Schema ID and configure the correct Schema ID.";
        }

        if (d.indexOf("payload appears to be base64 text") >= 0) {
            return "\n1 - The Kafka record value is Base64 text, not Avro binary bytes."
                    + "\n2 - Decode the Base64 before producing to Kafka, or use a producer option that sends BINARY, not STRING."
                    + "\n3 - The adapter expects bytes, not the textual Base64 representation.";
        }

        if (d.indexOf("invalid avro string length") >= 0
                || d.indexOf("payload ended unexpectedly") >= 0
                || d.indexOf("invalid avro bytes length") >= 0) {
            if ("FixedSchemaID".equalsIgnoreCase(mode)) {
                return "\n1 - The channel is forcing Fixed Schema ID " + String.valueOf(schemaId) + ", but the payload bytes do not match that schema."
                        + "\n2 - If the payload has a Confluent header with another Schema ID, switch Schema ID Source to Magic Byte."
                        + "\n3 - If you want Fixed Schema ID, produce raw Avro binary encoded with exactly schema " + String.valueOf(schemaId) + ", or a Confluent wire payload whose header also contains " + String.valueOf(schemaId) + "."
                        + "\n4 - Do not edit Avro binary/Base64 manually; one wrong length byte corrupts the decoder alignment.";
            }
            return "\n1 - The payload is not valid Avro for the schema selected by the adapter."
                    + "\n2 - Confirm the payload Schema ID, schema field order, and whether the record is Confluent wire format or raw Avro binary.";
        }

        return "\n1 - Check whether the channel is configured for Magic Byte or Fixed Schema ID."
                + "\n2 - Check whether the Kafka record value is real binary Avro, not JSON text or Base64 text."
                + "\n3 - Check whether the schema used to produce the payload is the same schema used by the adapter to decode it.";
    }

    private static final char[] HEX = "0123456789ABCDEF".toCharArray();

    private String buildFirstBytesHex(final byte[] payloadBytes, final int maxBytes) {
        if (payloadBytes == null || payloadBytes.length == 0) {
            return "EMPTY";
        }
        final int limit = Math.min(payloadBytes.length, Math.max(1, maxBytes));
        final StringBuilder out = new StringBuilder(limit * 3);
        for (int i = 0; i < limit; i++) {
            if (i > 0) out.append(' ');
            appendHexByte(out, payloadBytes[i]);
        }
        return out.toString();
    }

    private static void appendHexByte(final StringBuilder out, final byte value) {
        final int v = value & 0xFF;
        out.append(HEX[v >>> 4]).append(HEX[v & 0x0F]);
    }

    private Exchange markError(Exchange exchange, byte[] rawBody, String errCode, String format, String mode, int schemaId, String detail) {
        exchange.getIn().setBody(rawBody);
        exchange.getIn().setHeader("x-sdiakafka-payload-first-bytes-hex", buildFirstBytesHex(rawBody, cachedSchemaHeaderPreviewBytes));
        exchange.getIn().setHeader("x-sdiakafka-error-code", errCode);
        exchange.getIn().setHeader("x-sdiakafka-error-detail", detail);
        exchange.getIn().setHeader("x-sdiakafka-resolved-schema-id", schemaId);
        exchange.getIn().setHeader("x-sdiakafka-schema-mode", mode);
        exchange.setException(new RuntimeCamelException(errCode));
        return exchange;
    }

    private boolean isNoOffsetForPartitionFailure(Throwable t) {
        while (t != null) {
            final String className = t.getClass().getName();
            final String msg = t.getMessage();
            if (className != null && className.indexOf("NoOffsetForPartitionException") >= 0) {
                return true;
            }
            if (msg != null) {
                final String lower = msg.toLowerCase(java.util.Locale.ROOT);
                if (lower.indexOf("no offset") >= 0 && lower.indexOf("partition") >= 0) {
                    return true;
                }
            }
            t = t.getCause();
        }
        return false;
    }

    private String rootCauseMessage(Throwable t) {
        if (t == null) return "Unknown Error Context";
        while (t.getCause() != null) { t = t.getCause(); }
        return t.getMessage() != null ? t.getMessage() : t.getClass().getName();
    }

    private String normalizeConversionFormat(String format) {
        return format == null ? "None" : format.trim();
    }

    private String trimToNull(String str) {
        if (str == null) return null;
        String t = str.trim();
        return t.isEmpty() ? null : t;
    }

    private boolean isPlainTextAuthentication(final String authentication) {
        final String normalized = normalizeAuthenticationToken(authentication);
        if (normalized == null) {
            return false;
        }
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

    private String maskUrl(String url) {
        return url == null ? "<empty>" : url;
    }

    private List<String> parseTopicExpression(String expr) {
        if (expr == null) return Collections.emptyList();
        List<String> list = new ArrayList<>();
        for (String s : expr.split(",")) {
            String t = s.trim();
            if (!t.isEmpty()) list.add(t);
        }
        return list;
    }

    private boolean isPatternMode(String token) {
        return token.contains("*") || token.contains("^") || token.contains("$");
    }

    private boolean containsPatternMode(List<String> tokens) {
        for (String t : tokens) { if (isPatternMode(t)) return true; }
        return false;
    }

    private Pattern compileTopicPatternExpression(List<String> tokens) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tokens.size(); i++) {
            String t = tokens.get(i);
            String regex = t.replace(".", "\\.").replace("*", ".*");
            sb.append("(").append(regex).append(")");
            if (i < tokens.size() - 1) sb.append("|");
        }
        return Pattern.compile(sb.toString());
    }

    private List<String> findMissingTopics(Map<String, List<PartitionInfo>> known, List<String> targets) {
        List<String> missing = new ArrayList<>();
        if (known == null) return targets;
        for (String t : targets) { if (!known.containsKey(t)) missing.add(t); }
        return missing;
    }

    private List<TopicPartition> parsePartitions(String topic, String pParam) {
        List<TopicPartition> list = new ArrayList<>();
        for (String s : pParam.split(",")) {
            list.add(new TopicPartition(topic, Integer.parseInt(s.trim())));
        }
        return list;
    }

    private boolean shouldUseMagicByte(String mode, String fixedId, byte[] payload) {
        final String normalizedMode = normalizeSchemaIdResolutionMode(mode);

        if ("MAGIC".equals(normalizedMode)) {
            return true;
        }
        if ("FIXED".equals(normalizedMode)) {
            return false;
        }

        // Defensive fallback for CPI/ADK metadata binding drift:
        // if the channel has a Schema ID filled, do NOT silently auto-detect Magic Byte.
        // Otherwise a Fixed Schema ID channel can accidentally decode a 100001 payload
        // through its Confluent header while the UI shows 100007.
        if (trimToNull(fixedId) != null) {
            return false;
        }

        return (payload != null && payload.length > 0 && payload[0] == 0x00);
    }

    private String normalizeSchemaIdResolutionMode(String mode) {
        final String raw = trimToNull(mode);
        if (raw == null) {
            return "";
        }

        final String m = raw.toLowerCase();

        // Accept both internal values and UI labels. CPI may persist labels differently
        // across adapter metadata refreshes, especially after ESA redeploys.
        if (m.indexOf("fixed") >= 0 || m.indexOf("schemaid") >= 0 || m.indexOf("schema id") >= 0) {
            return "FIXED";
        }
        if (m.indexOf("magic") >= 0 || m.indexOf("wire") >= 0 || m.indexOf("header") >= 0) {
            return "MAGIC";
        }
        return "";
    }

    private void registerRuntimeStatus(String status, ConsumerRecord<String, byte[]> r, Throwable t) {}

    private void fireChannelFailed(Exchange exchange, String errorMsg) {
        try {
            AdapterMessageLogFactory factory =
                    ITApiFactory.getApi(AdapterMessageLogFactory.class, null);
            if (factory == null) {
                LOG.warn("[SDIA Kafka] AdapterMessageLogFactory not available — channel status icon not updated.");
                return;
            }
            String topic = endpoint.getTopicPattern() != null ? endpoint.getTopicPattern() : "unknown-topic";
            String host  = endpoint.resolveBootstrapServers() != null ? endpoint.resolveBootstrapServers() : "unknown-host";
            AdapterMessageLogWithStatus msgLog =
                    factory.getMessageLogWithStatus(
                            exchange.getIn(),
                            host,
                            topic,
                            "EventSmartKafka"
                    );
            if (msgLog != null) {
                msgLog.fireStatusEvent(AdapterStatusEvent.FAILED);
            }
        } catch (Throwable t) {
            LOG.warn("[SDIA Kafka] Could not fire FAILED channel status: " + t.getMessage());
        }
    }


    private boolean isOnPremiseProxyMode() {
        final String proxy = trimToNull(endpoint.getProxyType());
        if (proxy == null) {
            return false;
        }
        final String normalized = proxy.replace('_', ' ').replace('-', ' ').toLowerCase();
        return normalized.indexOf("on premise") >= 0 || normalized.indexOf("onpremise") >= 0;
    }

    private void applyOnPremiseClientBootstrapIfRequired(final Properties props,
                                                         final String kafkaClientBootstrapServers) {
        if (!isOnPremiseProxyMode()) {
            return;
        }
        if (props == null) {
            return;
        }
        final String bootstrap = trimToNull(kafkaClientBootstrapServers);
        if (bootstrap == null) {
            return;
        }
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        // Keep Kafka metadata refresh aggressive during Cloud Connector tests.
        props.put("metadata.max.age.ms", "10000");
    }

    private synchronized String ensureOnPremiseTcpTunnel(final String virtualBootstrapServers) throws Exception {
        final String current = trimToNull(onPremiseClientBootstrapServers);
        if (onPremiseTcpTunnel != null && onPremiseTcpTunnel.isRunning() && current != null) {
            return current;
        }

        closeOnPremiseTcpTunnelSilently();

        final List<BootstrapAddress> targets = parseBootstrapAddresses(virtualBootstrapServers);
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("No valid virtual bootstrap host:port found for On-Premise mode: " + virtualBootstrapServers);
        }

        final String locationId = trimToNull(endpoint.getSapSapccLocationId());

        final SdiaCloudConnectorTcpTunnel.TunnelConfig tunnelConfig =
                SdiaCloudConnectorTcpProbe.resolveAndProbe(virtualBootstrapServers, locationId);

        final SdiaCloudConnectorTcpTunnel tunnel = new SdiaCloudConnectorTcpTunnel(tunnelConfig);
        tunnel.start();

        this.onPremiseTcpTunnel = tunnel;
        this.onPremiseClientBootstrapServers = tunnel.getLocalBootstrapServers();

        LOG.warn("[SDIA Kafka] SAP Cloud Connector TCP tunnel ready. "
                + "virtualBootstrap=" + virtualBootstrapServers
                + " | kafkaClientBootstrap=" + this.onPremiseClientBootstrapServers
                + " | locationId=" + displayOptionalLocationId(locationId)
                + " | cloudModeUnaffected=true");

        return this.onPremiseClientBootstrapServers;
    }


    private void closeOnPremiseTcpTunnelSilently() {
        final SdiaCloudConnectorTcpTunnel tunnel = this.onPremiseTcpTunnel;
        this.onPremiseTcpTunnel = null;
        this.onPremiseClientBootstrapServers = null;
        if (tunnel != null) {
            try { tunnel.close(); } catch (Throwable ignored) {}
        }
    }

    private String maskLocationId(final String locationId) {
        final String value = trimToNull(locationId);
        if (value == null) {
            return "<empty>";
        }
        if (value.length() <= 2) {
            return "**";
        }
        return value.charAt(0) + "***" + value.charAt(value.length() - 1);
    }

    private String displayOptionalLocationId(final String locationId) {
        final String value = trimToNull(locationId);
        if (value == null) {
            return "<not configured - optional/default Cloud Connector>";
        }
        return maskLocationId(value);
    }

    private List<BootstrapAddress> parseBootstrapAddresses(final String bootstrapServers) {
        final List<BootstrapAddress> result = new ArrayList<BootstrapAddress>(4);
        final String value = trimToNull(bootstrapServers);
        if (value == null) {
            return result;
        }

        int start = 0;
        final int len = value.length();
        while (start < len) {
            int comma = value.indexOf(',', start);
            if (comma == -1) {
                comma = len;
            }
            final String entry = trimToNull(value.substring(start, comma));
            if (entry != null) {
                final BootstrapAddress parsed = parseBootstrapAddress(entry);
                if (parsed != null) {
                    result.add(parsed);
                }
            }
            start = comma + 1;
        }
        return result;
    }

    private BootstrapAddress parseBootstrapAddress(final String entry) {
        final String value = trimToNull(entry);
        if (value == null) {
            return null;
        }
        final int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon >= value.length() - 1) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap entry for On-Premise mode. Expected host:port, got: " + value);
        }
        final String host = trimToNull(value.substring(0, colon));
        final String portStr = trimToNull(value.substring(colon + 1));
        if (host == null || portStr == null) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap entry for On-Premise mode. Expected host:port, got: " + value);
        }
        try {
            final int port = Integer.parseInt(portStr);
            if (port <= 0 || port > 65535) {
                throw new NumberFormatException("port out of range");
            }
            return new BootstrapAddress(host, port);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid Kafka bootstrap port for On-Premise mode. Entry: " + value, e);
        }
    }

    private static final class BootstrapAddress {
        final String host;
        final int port;

        BootstrapAddress(final String host, final int port) {
            this.host = host;
            this.port = port;
        }

        String authority() {
            return host + ':' + port;
        }
    }

    private void closeKafkaConsumerSilently() {
        if (this.kafkaConsumer != null) {
            try { this.kafkaConsumer.wakeup(); } catch (Throwable ignored) {}
            try { this.kafkaConsumer.close(Duration.ZERO); } catch (Exception e) {}
            this.kafkaConsumer = null;
        }
    }

    private void validateConfiguration() {}

    @Override
    protected void doStop() throws Exception {
        closed.set(true);
        closeKafkaConsumerSilently();
        closeOnPremiseTcpTunnelSilently();
        super.doStop();
    }
}