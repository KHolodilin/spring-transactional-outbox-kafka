package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
@EmbeddedKafka(partitions = 1, topics = EventConstants.TOPIC_ORDERS)
class IdempotencyConflictIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void sameKeyDifferentBodyReturns409() {
        String idempotencyKey = UUID.randomUUID().toString();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(EventConstants.IDEMPOTENCY_KEY_HEADER, idempotencyKey);

        Map<String, Object> firstBody = Map.of(
                "customerId", 8,
                "items", List.of(Map.of("productId", "sku-1", "quantity", 1, "price", 5.00))
        );
        Map<String, Object> secondBody = Map.of(
                "customerId", 8,
                "items", List.of(Map.of("productId", "sku-2", "quantity", 1, "price", 5.00))
        );

        restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(firstBody, headers), Map.class);
        ResponseEntity<ProblemDetail> conflict = restTemplate.postForEntity(
                "/api/v1/orders",
                new HttpEntity<>(secondBody, headers),
                ProblemDetail.class
        );

        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);
    }
}
