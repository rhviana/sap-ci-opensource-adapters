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