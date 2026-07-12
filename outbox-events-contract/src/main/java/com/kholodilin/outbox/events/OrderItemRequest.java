package com.kholodilin.outbox.events;

import com.fasterxml.jackson.annotation.JsonInclude;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/** Single line item inside {@link CreateOrderRequest}. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class OrderItemRequest {

    /** Stock-keeping unit or product identifier in the catalog. */
    @NotBlank
    private String productId;

    /** Number of units ordered; must be greater than zero. */
    @Positive
    private int quantity;

    /** Unit price at order time; must be positive. */
    @NotNull
    @Positive
    private BigDecimal price;
}
