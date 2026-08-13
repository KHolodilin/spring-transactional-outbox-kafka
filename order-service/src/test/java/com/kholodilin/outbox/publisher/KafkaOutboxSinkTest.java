package com.kholodilin.outbox.publisher;

import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaOutboxSinkTest {

    @Mock
    private KafkaBatchPublisher kafkaBatchPublisher;

    private KafkaOutboxSink sink;

    @BeforeEach
    void setUp() {
        sink = new KafkaOutboxSink(kafkaBatchPublisher, JsonMapper.builder().build());
    }

    @Test
    void publishMapsPayloadAndReturnsAllSucceeded() {
        OutboxRecord record = record(
                "{\"orderId\":7,\"customerId\":42,\"correlationId\":\"corr-1\"}",
                Map.of("orderId", "7"));

        OutboxPublishResult result = sink.publish(List.of(record));

        assertThat(result).isInstanceOf(OutboxPublishResult.AllSucceeded.class);
        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<EventEnvelope>> captor = ArgumentCaptor.forClass(List.class);
        verify(kafkaBatchPublisher).publish(captor.capture());
        EventEnvelope envelope = captor.getValue().getFirst();
        assertThat(envelope.eventId()).isEqualTo(11L);
        assertThat(envelope.orderId()).isEqualTo(7L);
        assertThat(envelope.customerId()).isEqualTo(42L);
        assertThat(envelope.eventType()).isEqualTo("OrderCreated");
        assertThat(envelope.correlationId()).isEqualTo("corr-1");
        assertThat(envelope.traceParent()).isEqualTo("00-aa-bb-01");
        assertThat(envelope.payload()).containsEntry("orderId", 7);
    }

    @Test
    void publishFallsBackToHeaderCorrelationId() {
        OutboxRecord record = record(
                "{\"orderId\":7}",
                Map.of("correlationId", "from-header"));

        sink.publish(List.of(record));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<EventEnvelope>> captor = ArgumentCaptor.forClass(List.class);
        verify(kafkaBatchPublisher).publish(captor.capture());
        assertThat(captor.getValue().getFirst().correlationId()).isEqualTo("from-header");
    }

    @Test
    void publishLeavesCorrelationIdNullWhenMissing() {
        OutboxRecord record = record("{\"orderId\":7}", Map.of());

        sink.publish(List.of(record));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<EventEnvelope>> captor = ArgumentCaptor.forClass(List.class);
        verify(kafkaBatchPublisher).publish(captor.capture());
        assertThat(captor.getValue().getFirst().correlationId()).isNull();
    }

    @Test
    void publishReturnsAllFailedWhenKafkaThrows() {
        doThrow(new IllegalStateException("broker down")).when(kafkaBatchPublisher).publish(anyList());

        OutboxPublishResult result = sink.publish(List.of(record("{\"orderId\":1}", Map.of())));

        assertThat(result).isInstanceOf(OutboxPublishResult.AllFailed.class);
        assertThat(((OutboxPublishResult.AllFailed) result).cause()).hasMessage("broker down");
    }

    @Test
    void publishReturnsAllFailedWhenPayloadIsInvalidJson() {
        OutboxPublishResult result = sink.publish(List.of(record("not-json", Map.of())));

        assertThat(result).isInstanceOf(OutboxPublishResult.AllFailed.class);
        assertThat(((OutboxPublishResult.AllFailed) result).cause())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Failed to parse outbox payload JSON");
    }

    @Test
    void publishUsesNullCorrelationIdWhenPayloadMapIsNull() {
        ObjectMapper objectMapper = mock(ObjectMapper.class);
        sink = new KafkaOutboxSink(kafkaBatchPublisher, objectMapper);
        when(objectMapper.readValue(eq("{}"), any(TypeReference.class))).thenReturn(null);

        sink.publish(List.of(record("{}", null)));

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<EventEnvelope>> captor = ArgumentCaptor.forClass(List.class);
        verify(kafkaBatchPublisher).publish(captor.capture());
        assertThat(captor.getValue().getFirst().correlationId()).isNull();
    }

    private static OutboxRecord record(String payloadJson, Map<String, String> headers) {
        return new OutboxRecord(
                "default",
                11L,
                "OrderCreated",
                "7",
                "42",
                payloadJson,
                headers,
                "00-aa-bb-01",
                0,
                Instant.parse("2026-08-13T12:00:00Z"));
    }
}
