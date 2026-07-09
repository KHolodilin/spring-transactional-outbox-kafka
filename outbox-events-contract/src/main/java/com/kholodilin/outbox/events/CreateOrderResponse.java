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

    private Long orderId;
    private Long eventId;
    private String status;
    private Instant createdAt;
}
