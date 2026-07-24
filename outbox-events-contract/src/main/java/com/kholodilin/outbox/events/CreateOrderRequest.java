package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.util.List;

/** Request body for {@code POST /api/v1/orders}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateOrderRequest(
        /** Customer who places the order; also used as the Kafka partition key. */
        @NotNull
        @Positive
        Long customerId,

        /** Line items that make up the order; must contain at least one element. */
        @NotEmpty
        List<@Valid OrderItemRequest> items,

        /** Optional trace id propagated to logs, outbox payload, and Kafka headers. */
        String correlationId
) {
}
