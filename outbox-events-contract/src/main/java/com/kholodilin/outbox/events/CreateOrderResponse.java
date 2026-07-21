package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.Instant;

/** Successful order creation response returned by {@code order-service}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateOrderResponse(
        /** Identifier of the persisted order row. */
        Long orderId,

        /** Identifier of the outbox event scheduled for Kafka publish. */
        Long eventId,

        /** Business status of the accepted order (for example {@code ACCEPTED}). */
        String status,

        /** UTC timestamp when the order was stored. */
        Instant createdAt
) {
}
