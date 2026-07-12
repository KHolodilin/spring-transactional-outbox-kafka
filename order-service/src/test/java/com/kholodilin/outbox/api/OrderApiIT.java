package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.events.EventEnvelope;
import com.kholodilin.outbox.events.OutboxStatus;
import com.kholodilin.outbox.publisher.KafkaBatchPublisher;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = EventConstants.TOPIC_ORDERS)
class OrderApiIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @MockitoSpyBean
    private KafkaBatchPublisher kafkaBatchPublisher;

    @Test
    void createsOrderAndPublishesKafkaMessageWithCustomerPartitionKey() {
        String idempotencyKey = UUID.randomUUID().toString();
        Map<String, Object> body = Map.of(
                "customerId", 42,
                "items", List.of(Map.of("productId", "sku-1", "quantity", 1, "price", 19.99)),
                "correlationId", "corr-it-1"
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(EventConstants.IDEMPOTENCY_KEY_HEADER, idempotencyKey);

        ResponseEntity<Map> response = restTemplate.postForEntity(
                "/api/v1/orders",
                new HttpEntity<>(body, headers),
                Map.class
        );

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody()).containsKeys("orderId", "eventId", "status");
        long eventId = ((Number) response.getBody().get("eventId")).longValue();

        awaitSentInDatabase(eventId);

        @SuppressWarnings("unchecked")
        ArgumentCaptor<List<EventEnvelope>> captor = ArgumentCaptor.forClass(List.class);
        verify(kafkaBatchPublisher, atLeastOnce()).publish(captor.capture());

        EventEnvelope published = captor.getAllValues().stream()
                .flatMap(List::stream)
                .filter(envelope -> envelope.getEventId() == eventId)
                .findFirst()
                .orElseThrow(() -> new AssertionError("No publish call for eventId=" + eventId));

        assertThat(published.getCustomerId()).isEqualTo(42L);
        // Kafka partition key is String.valueOf(customerId) — see KafkaBatchPublisher.
    }

    private void awaitSentInDatabase(long eventId) {
        long deadline = System.currentTimeMillis() + 20_000;
        while (System.currentTimeMillis() < deadline) {
            Integer status = jdbcTemplate.queryForObject(
                    "SELECT status FROM outbox_events WHERE id = ?",
                    Integer.class,
                    eventId
            );
            if (status != null && status == OutboxStatus.SENT.getCode()) {
                return;
            }
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting for outbox SENT status", ex);
            }
        }
        throw new AssertionError("Outbox event " + eventId + " was not marked SENT within 20s");
    }
}
