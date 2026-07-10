package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles({"test", "ratelimit"})
@EmbeddedKafka(partitions = 1, topics = EventConstants.TOPIC_ORDERS)
class RateLimitIT {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void burstRequestsReturn429() {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(EventConstants.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString());

        Map<String, Object> body = Map.of(
                "customerId", 99,
                "items", List.of(Map.of("productId", "sku-1", "quantity", 1, "price", 1.00))
        );

        ResponseEntity<Map> first = restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(body, headers), Map.class);
        headers.set(EventConstants.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString());
        ResponseEntity<String> second = restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(body, headers), String.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }
}
