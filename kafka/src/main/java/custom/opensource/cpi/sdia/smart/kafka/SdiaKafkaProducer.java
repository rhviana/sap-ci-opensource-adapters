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

import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.plain.PlainLoginModule;
import java.nio.charset.StandardCharsets;

/**
 * Minimal Kafka receiver-side producer.
 */
public class SdiaKafkaProducer extends DefaultProducer {


    private final SdiaKafkaEndpoint endpoint;
    private KafkaProducer<String, byte[]> kafkaProducer;

    public SdiaKafkaProducer(final SdiaKafkaEndpoint endpoint) {
        super(endpoint);
        this.endpoint = endpoint;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();

        validateConfiguration();

        KafkaProducerFactory factory = new KafkaProducerFactory();

        ClassLoader originalContextClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader kafkaClassLoader = PlainLoginModule.class.getClassLoader();
        if (kafkaClassLoader == null) {
            kafkaClassLoader = SdiaKafkaProducer.class.getClassLoader();
        }

        try {
            Thread.currentThread().setContextClassLoader(kafkaClassLoader);
            this.kafkaProducer = new KafkaProducer<String, byte[]>(factory.createProperties(endpoint));
        } finally {
            Thread.currentThread().setContextClassLoader(originalContextClassLoader);
        }
    }

    @Override
    protected void doStop() throws Exception {
        if (kafkaProducer != null) {
            try {
                kafkaProducer.flush();
                kafkaProducer.close();
            } catch (Exception ignored) {
            }
        }
        super.doStop();
    }

    @Override
    public void process(final Exchange exchange) throws Exception {
        if (kafkaProducer == null) {
            throw new IllegalStateException("KafkaProducer is not initialized.");
        }

        String topic = trimToNull(endpoint.getTargetTopic());
        String key = extractKey(exchange);
        byte[] body = exchange.getIn().getBody(byte[].class);
        if (body == null) {
            String asString = exchange.getIn().getBody(String.class);
            body = asString == null ? new byte[0] : asString.getBytes(StandardCharsets.UTF_8);
        }

        ProducerRecord<String, byte[]> record = new ProducerRecord<String, byte[]>(topic, key, body);
        RecordMetadata metadata = kafkaProducer.send(record).get();

        exchange.getIn().setHeader("SAP_KafkaTopic", metadata.topic());
        exchange.getIn().setHeader("SAP_KafkaPartition", metadata.partition());
        exchange.getIn().setHeader("SAP_KafkaOffset", metadata.offset());
    }

    private String extractKey(final Exchange exchange) {
        String keyHeaderName = trimToNull(endpoint.getKeyHeaderName());
        if (keyHeaderName == null) {
            return null;
        }
        Object value = exchange.getIn().getHeader(keyHeaderName);
        return value == null ? null : String.valueOf(value);
    }

    private void validateConfiguration() {
        if (trimToNull(endpoint.getFirstUriPart()) == null) {
            throw new IllegalArgumentException("Kafka Host is mandatory.");
        }
        if (trimToNull(endpoint.getTargetTopic()) == null) {
            throw new IllegalArgumentException("Kafka Target Topic is mandatory.");
        }
    }

    private static String trimToNull(final String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
