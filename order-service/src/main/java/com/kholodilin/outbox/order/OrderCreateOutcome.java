package com.kholodilin.outbox.order;

import com.kholodilin.outbox.events.CreateOrderResponse;

/**
 * Outcome of create-order: a newly persisted order ({@code created=true}) or an idempotent replay
 * of a completed response ({@code created=false}).
 */
public record OrderCreateOutcome(CreateOrderResponse response, boolean created) {
}
