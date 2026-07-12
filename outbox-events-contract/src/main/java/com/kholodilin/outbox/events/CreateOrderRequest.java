package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/** Request body for {@code POST /api/v1/orders}. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateOrderRequest {

    /** Customer who places the order; also used as the Kafka partition key. */
    @NotNull
    @Positive
    private Long customerId;

    /** Line items that make up the order; must contain at least one element. */
    @NotEmpty
    private List<@Valid OrderItemRequest> items;

    /** Optional trace id propagated to logs, outbox payload, and Kafka headers. */
    private String correlationId;
}
