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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureTestRestTemplate
@ActiveProfiles({"test", "ratelimit"})
class RateLimitIT extends AbstractIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void burstRequestsReturn429() throws Exception {
        Map<String, Object> body = Map.of(
                "customerId", 99,
                "items", List.of(Map.of("productId", "sku-1", "quantity", 1, "price", 1.00))
        );

        HttpHeaders firstHeaders = new HttpHeaders();
        firstHeaders.setContentType(MediaType.APPLICATION_JSON);
        firstHeaders.set(EventConstants.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString());

        HttpHeaders secondHeaders = new HttpHeaders();
        secondHeaders.setContentType(MediaType.APPLICATION_JSON);
        secondHeaders.set(EventConstants.IDEMPOTENCY_KEY_HEADER, UUID.randomUUID().toString());

        CompletableFuture<ResponseEntity<Map>> first = CompletableFuture.supplyAsync(() ->
                restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(body, firstHeaders), Map.class)
        );
        CompletableFuture<ResponseEntity<String>> second = CompletableFuture.supplyAsync(() ->
                restTemplate.postForEntity("/api/v1/orders", new HttpEntity<>(body, secondHeaders), String.class)
        );

        ResponseEntity<Map> firstResponse = first.get(30, TimeUnit.SECONDS);
        ResponseEntity<String> secondResponse = second.get(30, TimeUnit.SECONDS);

        // With capacity=1 buckets, either parallel request may win the token.
        assertThat(List.of(firstResponse.getStatusCode(), secondResponse.getStatusCode()))
                .containsExactlyInAnyOrder(HttpStatus.CREATED, HttpStatus.TOO_MANY_REQUESTS);
    }
}
