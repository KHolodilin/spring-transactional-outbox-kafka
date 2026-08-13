package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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
@RequiredArgsConstructor
public class KafkaOutboxSink implements OutboxSink {

    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<>() {
    };

    private final KafkaBatchPublisher kafkaBatchPublisher;
    private final ObjectMapper objectMapper;

    @Override
    public OutboxPublishResult publish(List<OutboxRecord> batch) {
        try {
            List<EventEnvelope> envelopes = batch.stream().map(this::toEnvelope).toList();
            kafkaBatchPublisher.publish(envelopes);
            return new OutboxPublishResult.AllSucceeded();
        } catch (Exception ex) {
            log.warn("Kafka outbox publish failed for batchSize={}", batch.size(), ex);
            return new OutboxPublishResult.AllFailed(ex);
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
