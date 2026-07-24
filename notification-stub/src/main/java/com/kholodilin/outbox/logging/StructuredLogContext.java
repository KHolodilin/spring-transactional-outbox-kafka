package com.kholodilin.outbox.logging;

import org.slf4j.MDC;

/** Structured logging helpers for notification-stub MDC fields. */
public final class StructuredLogContext {

    private StructuredLogContext() {
    }

    /** Sets {@code event.action} for OpenSearch / JSON log filtering. */
    public static void putEventAction(String eventAction) {
        if (eventAction != null) {
            MDC.put("event.action", eventAction);
        }
    }

    /**
     * Sets correlation and customer fields from a consumed event.
     *
     * @param correlationId optional correlation id from the envelope
     * @param customerId    customer id (also written as {@code customer.id})
     */
    public static void putCorrelation(String correlationId, Long customerId) {
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
        if (customerId != null) {
            String value = String.valueOf(customerId);
            MDC.put("customerId", value);
            MDC.put("customer.id", value);
        }
    }

    /**
     * Sets order / outbox identifiers for a single consumed event.
     *
     * @param orderId  business order id
     * @param outboxId outbox / event id
     */
    public static void putOrderFields(Long orderId, Long outboxId) {
        if (orderId != null) {
            MDC.put("order.id", String.valueOf(orderId));
        }
        if (outboxId != null) {
            MDC.put("outbox.id", String.valueOf(outboxId));
        }
    }

    /** Sets {@code event.type} from the envelope. */
    public static void putEventType(String eventType) {
        if (eventType != null) {
            MDC.put("event.type", eventType);
        }
    }

    /**
     * Sets mock notification channel / delivery status (demo fields only).
     *
     * @param channel e.g. {@code log}
     * @param status  e.g. {@code sent}
     */
    public static void putNotificationFields(String channel, String status) {
        if (channel != null) {
            MDC.put("notification.channel", channel);
        }
        if (status != null) {
            MDC.put("notification.status", status);
        }
    }

    /** Sets {@code duration.ms} for batch processing time. */
    public static void putDurationMs(long durationMs) {
        MDC.put("duration.ms", String.valueOf(durationMs));
    }

    /** Sets {@code outbox.batch_size} for the current Kafka poll batch. */
    public static void putBatchSize(int batchSize) {
        MDC.put("outbox.batch_size", String.valueOf(batchSize));
    }

    /**
     * Sets Kafka coordinates for the current consumer record.
     *
     * @param topic     topic name
     * @param partition partition id
     * @param offset    record offset
     */
    public static void putKafkaFields(String topic, Integer partition, Long offset) {
        if (topic != null) {
            MDC.put("kafka.topic", topic);
        }
        if (partition != null) {
            MDC.put("kafka.partition", String.valueOf(partition));
        }
        if (offset != null) {
            MDC.put("kafka.offset", String.valueOf(offset));
        }
    }

    /** Sets {@code instance.id} from stub configuration. */
    public static void putInstanceFields(String instanceId) {
        if (instanceId != null) {
            MDC.put("instance.id", instanceId);
        }
    }

    /**
     * Copies Micrometer {@code traceId}/{@code spanId} into dotted aliases for OpenSearch.
     */
    public static void enrichTracingAliases() {
        String traceId = MDC.get("traceId");
        if (traceId != null) {
            MDC.put("trace.id", traceId);
        }
        String spanId = MDC.get("spanId");
        if (spanId != null) {
            MDC.put("span.id", spanId);
        }
    }

    /**
     * Clears per-record / per-batch consumer MDC keys after processing.
     * <p>
     * Callers typically re-apply instance fields afterwards.
     */
    public static void clearConsumerContext() {
        MDC.remove("correlationId");
        MDC.remove("customerId");
        MDC.remove("customer.id");
        MDC.remove("order.id");
        MDC.remove("outbox.id");
        MDC.remove("event.type");
        MDC.remove("event.action");
        MDC.remove("duration.ms");
        MDC.remove("kafka.topic");
        MDC.remove("kafka.partition");
        MDC.remove("kafka.offset");
        MDC.remove("notification.channel");
        MDC.remove("notification.status");
    }
}
