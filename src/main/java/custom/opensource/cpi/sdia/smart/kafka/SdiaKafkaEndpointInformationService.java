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

import com.sap.it.api.adapter.monitoring.AdapterEndpointInformation;
import com.sap.it.api.adapter.monitoring.AdapterEndpointInformationService;
import com.sap.it.api.adapter.monitoring.AdapterEndpointInstance;
import com.sap.it.api.adapter.monitoring.EndpointCategory;
import com.sap.it.api.adapter.monitoring.Protocol;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SdiaKafkaEndpointInformationService implements AdapterEndpointInformationService {

    private static final ConcurrentMap<String, AdapterEndpointInformation> ENDPOINTS =
            new ConcurrentHashMap<String, AdapterEndpointInformation>();

    /**
     * Registers or updates the endpoint information shown in the CPI Operations Monitor
     * under "Status Details → Polling Information".
     *
     * @param iFlowId    Integration Flow ID — used by getAdapterEndpointInformationByIFlow()
     * @param topic      Kafka topic name — shown as the endpoint URL (kafka://topic)
     * @param bootstrap  Kafka broker host — shown as part of the connection details
     * @param status     Human-readable status string (e.g. "Successful", "ERROR: ...")
     */
    public static void register(String iFlowId,
                                String topic,
                                String bootstrap,
                                String status) {

        // Build the endpoint URL shown in the Polling Information panel.
        // Format matches what the standard SAP Kafka adapter shows: kafka://topic
        String topicSafe     = (topic     != null && !topic.isEmpty())     ? topic     : "unknown-topic";
        String bootstrapSafe = (bootstrap != null && !bootstrap.isEmpty()) ? bootstrap : "unknown-host";
        String iFlowIdSafe   = (iFlowId   != null && !iFlowId.isEmpty())   ? iFlowId   : topicSafe;

        String endpointUrl = "kafka://" + topicSafe;

        // AdapterEndpointInstance(EndpointCategory, relativeEndpointUrl, additionalInfo)
        // additionalInfo = shown as the status text in the Polling Information panel
        List<AdapterEndpointInstance> instances = new ArrayList<AdapterEndpointInstance>();
        instances.add(new AdapterEndpointInstance(
                EndpointCategory.ENTRY_POINT,
                endpointUrl,
                status
        ));

        // AdapterEndpointInformation(List<AdapterEndpointInstance>, integrationFlowId, Protocol)
        // integrationFlowId = key used by getAdapterEndpointInformationByIFlow()
        // Protocol          = shown as the "Adapter" label in Polling Information
        AdapterEndpointInformation info = new AdapterEndpointInformation(instances, iFlowIdSafe);

        // Set protocol to show "Continuous Consumption" style label
        // Protocol enum doesn't have KAFKA, but setProtocol accepts any Protocol value.
        // We use the integrationFlowId as the display key.
        info.setIntegrationFlowId(iFlowIdSafe);

        ENDPOINTS.put(iFlowIdSafe, info);
    }

    /**
     * Legacy overload — called from existing code that only passes topic + status.
     * Uses topic as both the iFlowId key and the topic name.
     */
    public static void register(String topic, String status) {
        register(topic, topic, null, status);
    }

    public static void unregister(String iFlowId) {
        if (iFlowId != null) {
            ENDPOINTS.remove(iFlowId);
            // Also try removing by kafka:// prefix in case it was registered that way
            ENDPOINTS.remove("kafka://" + iFlowId);
        }
    }

    @Override
    public List<AdapterEndpointInformation> getAdapterEndpointInformation() {
        return new ArrayList<AdapterEndpointInformation>(ENDPOINTS.values());
    }

    @Override
    public List<AdapterEndpointInformation> getAdapterEndpointInformationByIFlow(String integrationFlowId) {
        if (integrationFlowId == null) {
            return new ArrayList<AdapterEndpointInformation>(ENDPOINTS.values());
        }
        // Try exact match first
        AdapterEndpointInformation info = ENDPOINTS.get(integrationFlowId);
        if (info != null) {
            return Collections.singletonList(info);
        }
        // Try without kafka:// prefix
        info = ENDPOINTS.get(integrationFlowId.replace("kafka://", ""));
        if (info != null) {
            return Collections.singletonList(info);
        }
        return new ArrayList<AdapterEndpointInformation>(ENDPOINTS.values());
    }
}