package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Sends a batch of {@link EventEnvelope} messages to Kafka.
 * <p>
 * Partition key is {@code customerId} so all events for one customer stay ordered per partition.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaBatchPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final AppProperties properties;

    public void publish(List<EventEnvelope> envelopes) {
        String topic = properties.getKafka().getTopic();
        List<CompletableFuture<?>> futures = new ArrayList<>();
        for (EventEnvelope envelope : envelopes) {
            String key = String.valueOf(envelope.getCustomerId());
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, envelope);
            // Headers allow consumers to route/trace without parsing the JSON body.
            record.headers().add(new RecordHeader(EventConstants.HEADER_EVENT_ID, toBytes(envelope.getEventId())));
            record.headers().add(new RecordHeader(EventConstants.HEADER_ORDER_ID, toBytes(envelope.getOrderId())));
            record.headers().add(new RecordHeader(EventConstants.HEADER_CUSTOMER_ID, toBytes(envelope.getCustomerId())));
            if (envelope.getCorrelationId() != null) {
                record.headers().add(new RecordHeader(EventConstants.HEADER_CORRELATION_ID, envelope.getCorrelationId().getBytes(StandardCharsets.UTF_8)));
            }
            log.debug("Publishing to Kafka topic={} key={} eventId={}", topic, key, envelope.getEventId());
            futures.add(kafkaTemplate.send(record));
        }
        CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new)).join();
        // Block until the whole batch is acknowledged by Kafka (sync batch semantics).
        for (int i = 0; i < envelopes.size(); i++) {
            EventEnvelope envelope = envelopes.get(i);
            log.debug("Kafka publish completed eventId={} partitionKey={}", envelope.getEventId(), envelope.getCustomerId());
        }
    }

    private byte[] toBytes(Long value) {
        return String.valueOf(value).getBytes(StandardCharsets.UTF_8);
    }
}
