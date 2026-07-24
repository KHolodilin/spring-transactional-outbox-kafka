package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Sends a batch of {@link EventEnvelope} messages to Kafka.
 * <p>
 * Partition key is {@code customerId} so all events for one customer stay ordered per partition.
 * Restores W3C trace context from each envelope before publishing and injects {@code traceparent} headers.
 */
@Slf4j
@Component
public class KafkaBatchPublisher {

    private static final String TRACE_PARENT_KEY = "traceparent";
    private static final String TRACE_STATE_KEY = "tracestate";

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppProperties properties;
    private final TraceContextSupport traceContextSupport;
    private final ObjectProvider<Tracer> tracerProvider;
    private final ObjectProvider<Propagator> propagatorProvider;

    /**
     * @param kafkaTemplate      Spring Kafka template used for async sends
     * @param properties         topic and app settings
     * @param traceContextSupport restores per-envelope {@code traceparent} before send
     * @param tracerProvider     optional Micrometer tracer for header injection
     * @param propagatorProvider optional W3C propagator for Kafka headers
     */
    public KafkaBatchPublisher(
            KafkaTemplate<String, Object> kafkaTemplate,
            AppProperties properties,
            TraceContextSupport traceContextSupport,
            ObjectProvider<Tracer> tracerProvider,
            ObjectProvider<Propagator> propagatorProvider
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
        this.traceContextSupport = traceContextSupport;
        this.tracerProvider = tracerProvider;
        this.propagatorProvider = propagatorProvider;
    }

    /**
     * Publishes all envelopes and blocks until every send future completes.
     * <p>
     * Each message gets business headers ({@code eventId}, {@code orderId}, …) and,
     * when tracing is enabled, {@code traceparent} / {@code tracestate}. Failure of any
     * send fails the whole batch (caller marks rows FAILED / DEAD).
     *
     * @param envelopes non-empty batch claimed from the outbox
     */
    public void publish(List<EventEnvelope> envelopes) {
        String topic = properties.getKafka().getTopic();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (EventEnvelope envelope : envelopes) {
            traceContextSupport.runWithTraceParent(envelope.traceParent(), "outbox.publish", () -> {
                String key = String.valueOf(envelope.customerId());
                ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, envelope);
                addBusinessHeaders(record, envelope);
                addTraceHeaders(record);
                StructuredLogContext.putKafkaFields(topic, null, null);
                StructuredLogContext.putOrderFields(envelope.orderId(), envelope.eventId());
                log.debug("Publishing to Kafka topic={} key={} eventId={}", topic, key, envelope.eventId());
                futures.add(kafkaTemplate.send(record));
            });
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        for (EventEnvelope envelope : envelopes) {
            log.debug("Kafka publish completed eventId={} partitionKey={}", envelope.eventId(), envelope.customerId());
        }
    }

    private void addBusinessHeaders(ProducerRecord<String, Object> record, EventEnvelope envelope) {
        record.headers().add(new RecordHeader(EventConstants.HEADER_EVENT_ID, toBytes(envelope.eventId())));
        record.headers().add(new RecordHeader(EventConstants.HEADER_ORDER_ID, toBytes(envelope.orderId())));
        record.headers().add(new RecordHeader(EventConstants.HEADER_CUSTOMER_ID, toBytes(envelope.customerId())));
        if (envelope.correlationId() != null) {
            record.headers().add(new RecordHeader(
                    EventConstants.HEADER_CORRELATION_ID,
                    envelope.correlationId().getBytes(StandardCharsets.UTF_8)
            ));
        }
    }

    private void addTraceHeaders(ProducerRecord<String, Object> record) {
        Tracer tracer = tracerProvider.getIfAvailable();
        Propagator propagator = propagatorProvider.getIfAvailable();
        if (tracer == null || propagator == null) {
            return;
        }
        Span current = tracer.currentSpan();
        if (current == null) {
            return;
        }
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(current.context(), carrier, Map::put);
        String traceParent = carrier.get(TRACE_PARENT_KEY);
        if (traceParent != null) {
            record.headers().add(new RecordHeader(
                    EventConstants.HEADER_TRACEPARENT,
                    traceParent.getBytes(StandardCharsets.UTF_8)
            ));
        }
        String traceState = carrier.get(TRACE_STATE_KEY);
        if (traceState != null && !traceState.isBlank()) {
            record.headers().add(new RecordHeader(
                    EventConstants.HEADER_TRACESTATE,
                    traceState.getBytes(StandardCharsets.UTF_8)
            ));
        }
    }

    private byte[] toBytes(Long value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
}
