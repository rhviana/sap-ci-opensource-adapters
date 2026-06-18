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

import com.sap.it.api.adapter.monitoring.AdapterEndpointInformation;
import com.sap.it.api.adapter.monitoring.AdapterEndpointInformationService;
import com.sap.it.api.adapter.monitoring.AdapterEndpointInstance;
import com.sap.it.api.adapter.monitoring.EndpointCategory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Endpoint visualization bridge for SAP Cloud Integration.
 *
 * Important boundary:
 * SAP's public AdapterEndpointInformationService API is endpoint-visualization
 * only. It returns endpoint URLs used by the adapter. It is not the same
 * internal polling-status panel used by SAP-delivered adapters.
 *
 * For EventSmartKafka, exposing a fake HTTP endpoint such as
 *   https://<worker>/kafka://<topic>
 * is misleading because Kafka is consumed by a background polling consumer and
 * not by an inbound HTTP endpoint.
 *
 * Therefore endpoint visualization is disabled by default. Runtime state is
 * still kept in LAST_STATUS for internal debugging and future optional display,
 * but the CPI "Endpoints" card receives an empty list.
 */
public class SdiaKafkaEndpointInformationService implements AdapterEndpointInformationService {

    /**
     * Keep this false for release builds.
     *
     * If true, CPI may render an "Endpoints" card. That card is meant for URLs
     * relative to the worker node dispatcher. It is not Polling Information.
     */
    private static final boolean EXPOSE_ENDPOINT_VISUALIZATION = false;

    private static final ConcurrentMap<String, AdapterEndpointInformation> ENDPOINTS =
            new ConcurrentHashMap<String, AdapterEndpointInformation>();

    private static final ConcurrentMap<String, String> LAST_STATUS =
            new ConcurrentHashMap<String, String>();

    /**
     * Registers the latest runtime status.
     *
     * This method intentionally does not force a CPI endpoint visualization entry
     * when EXPOSE_ENDPOINT_VISUALIZATION=false. That removes the misleading
     * "https://.../kafka://topic" line from the deployed iFlow Endpoints panel.
     */
    public static void register(String iFlowId,
                                String topic,
                                String bootstrap,
                                String status) {

        final String topicSafe     = valueOr(topic, "unknown-topic");
        final String bootstrapSafe = valueOr(bootstrap, "unknown-bootstrap");
        final String iFlowIdSafe   = valueOr(iFlowId, topicSafe);
        final String statusSafe    = normalizeStatus(status);

        LAST_STATUS.put(iFlowIdSafe, statusSafe
                + " | topic=" + topicSafe
                + " | bootstrap=" + bootstrapSafe);

        if (!EXPOSE_ENDPOINT_VISUALIZATION) {
            ENDPOINTS.remove(iFlowIdSafe);
            ENDPOINTS.remove(topicSafe);
            ENDPOINTS.remove("kafka://" + topicSafe);
            return;
        }

        // Optional diagnostic mode only. The URL must be relative and start with '/'.
        // Do not use kafka:// here; SAP appends this relative URL to the worker URL.
        final String endpointUrl = "/eventsmartkafka/polling/" + safePathSegment(topicSafe);
        final String additionalInfo = buildAdditionalInfo(statusSafe, topicSafe, bootstrapSafe);

        final List<AdapterEndpointInstance> instances = new ArrayList<AdapterEndpointInstance>(1);
        instances.add(new AdapterEndpointInstance(
                EndpointCategory.ENTRY_POINT,
                endpointUrl,
                additionalInfo
        ));

        final AdapterEndpointInformation info =
                new AdapterEndpointInformation(instances, iFlowIdSafe);
        info.setIntegrationFlowId(iFlowIdSafe);

        ENDPOINTS.put(iFlowIdSafe, info);
    }

    /**
     * Legacy overload — called from existing code that only passes topic + status.
     */
    public static void register(String topic, String status) {
        register(topic, topic, null, status);
    }

    public static void unregister(String iFlowId) {
        if (iFlowId != null) {
            ENDPOINTS.remove(iFlowId);
            LAST_STATUS.remove(iFlowId);
            ENDPOINTS.remove("kafka://" + iFlowId);
            LAST_STATUS.remove("kafka://" + iFlowId);
        }
    }

    public static String getLastStatus(String iFlowId) {
        if (iFlowId == null) {
            return null;
        }
        return LAST_STATUS.get(iFlowId);
    }

    @Override
    public List<AdapterEndpointInformation> getAdapterEndpointInformation() {
        if (!EXPOSE_ENDPOINT_VISUALIZATION) {
            return Collections.emptyList();
        }
        return new ArrayList<AdapterEndpointInformation>(ENDPOINTS.values());
    }

    @Override
    public List<AdapterEndpointInformation> getAdapterEndpointInformationByIFlow(String integrationFlowId) {
        if (!EXPOSE_ENDPOINT_VISUALIZATION) {
            return Collections.emptyList();
        }
        if (integrationFlowId == null) {
            return new ArrayList<AdapterEndpointInformation>(ENDPOINTS.values());
        }

        AdapterEndpointInformation info = ENDPOINTS.get(integrationFlowId);
        if (info != null) {
            return Collections.singletonList(info);
        }

        final String normalized = integrationFlowId.replace("kafka://", "");
        info = ENDPOINTS.get(normalized);
        if (info != null) {
            return Collections.singletonList(info);
        }

        return Collections.emptyList();
    }

    private static String normalizeStatus(final String value) {
        final String v = valueOr(value, "Polling status unknown");
        if ("Successful".equalsIgnoreCase(v)) {
            return "Polling successful";
        }
        if ("Starting...".equalsIgnoreCase(v) || "Starting".equalsIgnoreCase(v)) {
            return "Polling starting";
        }
        return v;
    }

    private static String buildAdditionalInfo(final String status,
                                              final String topic,
                                              final String bootstrap) {
        return status + " | topic=" + topic + " | bootstrap=" + bootstrap;
    }

    private static String safePathSegment(final String value) {
        final String v = valueOr(value, "unknown");
        final StringBuilder sb = new StringBuilder(v.length());
        for (int i = 0; i < v.length(); i++) {
            final char c = v.charAt(i);
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '-' || c == '_' || c == '.') {
                sb.append(c);
            } else {
                sb.append('-');
            }
        }
        return sb.toString();
    }

    private static String valueOr(final String value, final String fallback) {
        if (value == null) {
            return fallback;
        }
        final String trimmed = value.trim();
        return trimmed.length() == 0 ? fallback : trimmed;
    }
}
