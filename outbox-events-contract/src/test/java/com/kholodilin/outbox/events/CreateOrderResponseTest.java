package com.kholodilin.outbox.events;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class CreateOrderResponseTest {

    @Test
    void storesOrderAndEventIdentifiers() {
        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        CreateOrderResponse response = new CreateOrderResponse(10L, 20L, "ACCEPTED", createdAt);

        assertThat(response.orderId()).isEqualTo(10L);
        assertThat(response.eventId()).isEqualTo(20L);
        assertThat(response.status()).isEqualTo("ACCEPTED");
        assertThat(response.createdAt()).isEqualTo(createdAt);
    }
}
