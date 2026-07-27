package com.kholodilin.outbox.api;

import com.kholodilin.outbox.events.EventConstants;
import com.kholodilin.outbox.support.AbstractIntegrationTest;
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
import org.springframework.test.context.ActiveProfiles;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles("test")
class IdempotencyIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void duplicateKeyReturnsCachedResponse() {
        String idempotencyKey = UUID.randomUUID().toString();
        Map<String, Object> body = Map.of(
                "customerId", 7,
                "items", List.of(Map.of("productId", "sku-1", "quantity", 1, "price", 5.00))
        );

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set(EventConstants.IDEMPOTENCY_KEY_HEADER, idempotencyKey);

        ResponseEntity<Map> first = restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(body, headers), Map.class);
        ResponseEntity<Map> second = restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(body, headers), Map.class);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(second.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(second.getBody().get("orderId")).isEqualTo(first.getBody().get("orderId"));
    }
}
