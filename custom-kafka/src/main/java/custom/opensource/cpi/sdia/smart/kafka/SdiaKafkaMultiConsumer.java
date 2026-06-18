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
import org.apache.camel.Processor;
import org.apache.camel.support.DefaultConsumer;

/**
 * Parallel consumer coordinator for the EventSmartKafka adapter.
 *
 * <p>When {@code parallelConsumers > 1}, the endpoint creates a single
 * {@link SdiaKafkaMultiConsumer} that internally manages N instances of
 * {@link SdiaKafkaConsumer}, all sharing the same {@code group.id}.
 *
 * <p>Kafka distributes topic partitions among the N consumer instances via
 * the standard group rebalance protocol. Effective parallelism is therefore
 * bounded by the number of partitions: if {@code parallelConsumers=4} but
 * the topic has only 2 partitions, only 2 consumers will be active at any
 * given time.
 *
 * <p>Lifecycle: {@code doStart()} starts all child consumers sequentially.
 * {@code doStop()} stops them all. If any child fails to start, the others
 * are stopped and the failure is propagated.
 */
public final class SdiaKafkaMultiConsumer extends DefaultConsumer {

    private final List<SdiaKafkaConsumer> consumers;

    public SdiaKafkaMultiConsumer(final SdiaKafkaEndpoint endpoint,
                                   final Processor processor,
                                   final int count) {
        super(endpoint, processor);
        this.consumers = new ArrayList<SdiaKafkaConsumer>(count);
        for (int i = 0; i < count; i++) {
            final SdiaKafkaConsumer c = new SdiaKafkaConsumer(endpoint, processor);
            c.setScheduledExecutorService(endpoint.getCamelContext()
                    .getExecutorServiceManager()
                    .newScheduledThreadPool(this,
                            "sdia-kafka-consumer-" + i,
                            1));
            consumers.add(c);
        }
    }

    @Override
    protected void doStart() throws Exception {
        int started = 0;
        try {
            for (SdiaKafkaConsumer c : consumers) {
                c.start();
                started++;
            }
        } catch (Exception e) {
            // Stop already-started consumers before propagating
            for (int i = 0; i < started; i++) {
                try { consumers.get(i).stop(); } catch (Throwable ignored) {}
            }
            throw e;
        }
    }

    @Override
    protected void doStop() throws Exception {
        Throwable first = null;
        for (SdiaKafkaConsumer c : consumers) {
            try { c.stop(); } catch (Throwable t) {
                if (first == null) first = t;
            }
        }
        if (first instanceof Exception) throw (Exception) first;
        if (first != null) throw new RuntimeException(first);
    }

    @Override
    protected void doSuspend() throws Exception {
        for (SdiaKafkaConsumer c : consumers) {
            try { c.suspend(); } catch (Throwable ignored) {}
        }
    }

    @Override
    protected void doResume() throws Exception {
        for (SdiaKafkaConsumer c : consumers) {
            try { c.resume(); } catch (Throwable ignored) {}
        }
    }

    public List<SdiaKafkaConsumer> getConsumers() {
        return consumers;
    }
}
