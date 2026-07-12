package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.KafkaProperties;
import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaBatchPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private KafkaBatchPublisher publisher;

    @BeforeEach
    void setUp() {
        AppProperties properties = new AppProperties();
        KafkaProperties kafka = new KafkaProperties();
        kafka.setTopic(EventConstants.TOPIC_ORDERS);
        properties.setKafka(kafka);
        publisher = new KafkaBatchPublisher(kafkaTemplate, properties);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(null, null)));
    }

    @Test
    void usesCustomerIdAsPartitionKey() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId(1L)
                .orderId(10L)
                .customerId(42L)
                .eventType(EventConstants.EVENT_TYPE_ORDER_CREATED)
                .payload(Map.of("orderId", 10))
                .occurredAt(Instant.parse("2026-07-12T10:00:00Z"))
                .build();

        publisher.publish(List.of(envelope));

        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        org.mockito.Mockito.verify(kafkaTemplate).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo(EventConstants.TOPIC_ORDERS);
        assertThat(captor.getValue().key()).isEqualTo("42");
        assertThat(captor.getValue().value()).isEqualTo(envelope);
    }
}
