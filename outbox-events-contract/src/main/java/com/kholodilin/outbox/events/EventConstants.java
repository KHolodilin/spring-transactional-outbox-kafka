package com.kholodilin.outbox.events;

/**
 * Shared Kafka topic name, message headers, and HTTP header names.
 * <p>
 * Kept in a separate module so {@code order-service} and {@code notification-stub}
 * use the same contract without depending on each other.
 */
public final class EventConstants {

    /** Kafka topic that carries order domain events. */
    public static final String TOPIC_ORDERS = "orders.events";

    /** Event type emitted when a new order is accepted. */
    public static final String EVENT_TYPE_ORDER_CREATED = "OrderCreated";

    /** Kafka / tracing header with the outbox event id. */
    public static final String HEADER_EVENT_ID = "eventId";

    /** Kafka / tracing header with the business order id. */
    public static final String HEADER_ORDER_ID = "orderId";

    /** Kafka / tracing header with the customer id. */
    public static final String HEADER_CUSTOMER_ID = "customerId";

    /** Kafka / tracing header with the client-supplied correlation id. */
    public static final String HEADER_CORRELATION_ID = "correlationId";

    /** W3C trace context propagated through Kafka (see {@code traceparent}). */
    public static final String HEADER_TRACEPARENT = "traceparent";

    /** Optional W3C tracestate header propagated through Kafka. */
    public static final String HEADER_TRACESTATE = "tracestate";

    /** HTTP header name for idempotent order creation requests. */
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    /** Prevents instantiation of this constants holder. */
    private EventConstants() {
    }
}
