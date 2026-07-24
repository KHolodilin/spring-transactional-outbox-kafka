/**
 * Shared domain event contract between {@code order-service} and {@code notification-stub}.
 * <p>
 * Contains Kafka envelope DTOs, outbox/idempotency status codes, and header/topic constants
 * so producer and consumer stay wire-compatible without a circular dependency.
 */
package com.kholodilin.outbox.events;
