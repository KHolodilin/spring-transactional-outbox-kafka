package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.ObservabilityVocabulary;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import com.kholodilin.outbox.tracing.TraceContextSupport;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * Default-channel {@link OutboxSink} that maps starter {@link OutboxRecord}s to {@link EventEnvelope}
 * and publishes via {@link KafkaBatchPublisher}.
 */
@Slf4j
@Component
public class KafkaOutboxSink implements OutboxSink {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final KafkaBatchPublisher kafkaBatchPublisher;
    private final ObjectMapper objectMapper;
    private final TraceContextSupport traceContextSupport;
    private final AppProperties appProperties;
    private final int maxRetries;

    public KafkaOutboxSink(
            KafkaBatchPublisher kafkaBatchPublisher,
            ObjectMapper objectMapper,
            TraceContextSupport traceContextSupport,
            AppProperties appProperties,
            @Value("${outbox.defaults.publisher.max-retries:5}") int maxRetries
    ) {
        this.kafkaBatchPublisher = kafkaBatchPublisher;
        this.objectMapper = objectMapper;
        this.traceContextSupport = traceContextSupport;
        this.appProperties = appProperties;
        this.maxRetries = maxRetries;
    }

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        long start = System.nanoTime();
        try {
            List<EventEnvelope> envelopes = traceContextSupport.runWithTraceParent(
                    null,
                    ObservabilityVocabulary.SPAN_BATCH_FETCH,
                    () -> mapLoadedBatch(batch)
            );
            String batchTrace = envelopes.isEmpty() ? null : envelopes.getFirst().traceParent();
            traceContextSupport.runWithTraceParent(batchTrace, ObservabilityVocabulary.SPAN_BATCH_PUBLISH, () -> {
                kafkaBatchPublisher.publish(envelopes);
                return null;
            });
            long durationMs = (System.nanoTime() - start) / 1_000_000;
            String completeTrace = batchTrace;
            traceContextSupport.runWithTraceParent(completeTrace, ObservabilityVocabulary.SPAN_BATCH_COMPLETE, () -> {
                StructuredLogContext.putDurationMs(durationMs);
                StructuredLogContext.putEventAction(ObservabilityVocabulary.OUTBOX_BATCH_PUBLISHED);
                log.info("Kafka batch published size={} durationMs={}", envelopes.size(), durationMs);
                return null;
            });
            return new OutboxPublishResult.AllSucceeded();
        } catch (Exception ex) {
            StructuredLogContext.putEventAction(ObservabilityVocabulary.OUTBOX_PUBLISH_FAILED);
            log.warn("Kafka outbox publish failed for batchSize={}", batch.size(), ex);
            logRetries(batch);
            return new OutboxPublishResult.AllFailed(ex);
        }
    }

    private List<EventEnvelope> mapLoadedBatch(List<OutboxRecord> batch) {
        StructuredLogContext.putInstanceFields(appProperties.getInstanceId());
        StructuredLogContext.enrichTracingAliases();
        StructuredLogContext.putBatchSize(batch.size());
        StructuredLogContext.putEventAction(ObservabilityVocabulary.OUTBOX_BATCH_LOADED);
        log.info("Outbox batch loaded size={}", batch.size());
        return batch.stream().map(this::toEnvelope).toList();
    }

    private void logRetries(List<OutboxRecord> batch) {
        for (OutboxRecord record : batch) {
            int nextRetry = record.retryCount() + 1;
            OutboxStatus status = nextRetry >= maxRetries ? OutboxStatus.DEAD : OutboxStatus.FAILED;
            StructuredLogContext.putOutboxStatus(status.name(), status.getCode(), nextRetry);
            StructuredLogContext.putEventAction(ObservabilityVocabulary.OUTBOX_RETRY);
            log.info("Outbox event marked {} eventId={} retryCount={}", status, record.eventId(), nextRetry);
        }
    }

    private EventEnvelope toEnvelope(OutboxRecord record) {
        Map<String, Object> payload = parsePayload(record.payloadJson());
        String correlationId = resolveCorrelationId(payload, record.headers());
        return new EventEnvelope(
                record.eventId(),
                Long.parseLong(record.aggregateId()),
                Long.parseLong(record.partitionKey()),
                record.eventType(),
                payload,
                correlationId,
                Instant.now(),
                record.traceParent()
        );
    }

    private Map<String, Object> parsePayload(String payloadJson) {
        try {
            return objectMapper.readValue(payloadJson, PAYLOAD_TYPE);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to parse outbox payload JSON", ex);
        }
    }

    private static String resolveCorrelationId(Map<String, Object> payload, Map<String, String> headers) {
        Object fromPayload = payload == null ? null : payload.get("correlationId");
        if (fromPayload != null) {
            return String.valueOf(fromPayload);
        }
        if (headers != null && headers.containsKey("correlationId")) {
            return headers.get("correlationId");
        }
        return null;
    }
}
