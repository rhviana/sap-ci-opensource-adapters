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

import java.util.Map;
import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;

/**
 * SDIA Kafka custom adapter component.
 */
public class SdiaKafkaComponent extends DefaultComponent {


    @Override
    protected Endpoint createEndpoint(final String uri, final String remaining, final Map<String, Object> parameters)
            throws Exception {
        final SdiaKafkaEndpoint endpoint = new SdiaKafkaEndpoint(uri, this);

        // remaining is the value after "EventSmartKafka://" in the Camel URI.
        // With AttributeTableMetadata for bootstrap servers, remaining may be empty
        // or a placeholder — the actual bootstrap servers are resolved via
        // endpoint.resolveBootstrapServers() from kafkaClusterHostsTable.
        // We still set firstUriPart as a display/legacy fallback.
        if (remaining != null && !remaining.trim().isEmpty()
                && !remaining.trim().equalsIgnoreCase("kafka")) {
            endpoint.setFirstUriPart(remaining.trim());
        }

        setProperties(endpoint, parameters);
        return endpoint;
    }
}