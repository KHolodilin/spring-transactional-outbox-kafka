package com.kholodilin.outbox.order;

import com.kholodilin.outbox.events.CreateOrderResponse;

/** Outcome of create-order: newly persisted ({@code created=true}) or idempotent replay. */
public record OrderCreateOutcome(CreateOrderResponse response, boolean created) {
}
