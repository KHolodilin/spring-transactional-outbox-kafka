package com.kholodilin.outbox.events;

/**
 * Shared Kafka topic name, message headers, and HTTP header names.
 * <p>
 * Kept in a separate module so {@code order-service} and {@code notification-stub}
 * use the same contract without depending on each other.
 */
public final class EventConstants {

    public static final String TOPIC_ORDERS = "orders.events";
    public static final String EVENT_TYPE_ORDER_CREATED = "OrderCreated";
    /** Kafka record key field — all events for one customer land in the same partition. */
    public static final String PARTITION_KEY_FIELD = "customerId";
    public static final String HEADER_EVENT_ID = "eventId";
    public static final String HEADER_ORDER_ID = "orderId";
    public static final String HEADER_CUSTOMER_ID = "customerId";
    public static final String HEADER_CORRELATION_ID = "correlationId";
    public static final String IDEMPOTENCY_KEY_HEADER = "Idempotency-Key";

    private EventConstants() {
    }
}
