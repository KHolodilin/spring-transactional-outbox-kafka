package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

/** Single line item inside {@link CreateOrderRequest}. */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record OrderItemRequest(
        /** Stock-keeping unit or product identifier in the catalog. */
        @NotBlank
        String productId,

        /** Number of units ordered; must be greater than zero. */
        @Positive
        int quantity,

        /** Unit price at order time; must be positive. */
        @NotNull
        @Positive
        BigDecimal price
) {
}
