package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

/** Successful order creation response returned by {@code order-service}. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateOrderResponse {

    /** Identifier of the persisted order row. */
    private Long orderId;

    /** Identifier of the outbox event scheduled for Kafka publish. */
    private Long eventId;

    /** Business status of the accepted order (for example {@code ACCEPTED}). */
    private String status;

    /** UTC timestamp when the order was stored. */
    private Instant createdAt;
}
