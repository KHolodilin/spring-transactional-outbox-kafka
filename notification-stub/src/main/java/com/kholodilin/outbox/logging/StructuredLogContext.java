package com.kholodilin.outbox.logging;

import org.slf4j.MDC;

/** Structured logging helpers for notification-stub MDC fields. */
public final class StructuredLogContext {

    private StructuredLogContext() {
    }

    public static void putEventAction(String eventAction) {
        if (eventAction != null) {
            MDC.put("event.action", eventAction);
        }
    }

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

    public static void putOrderFields(Long orderId, Long outboxId) {
        if (orderId != null) {
            MDC.put("order.id", String.valueOf(orderId));
        }
        if (outboxId != null) {
            MDC.put("outbox.id", String.valueOf(outboxId));
        }
    }

    public static void putEventType(String eventType) {
        if (eventType != null) {
            MDC.put("event.type", eventType);
        }
    }

    public static void putNotificationFields(String channel, String status) {
        if (channel != null) {
            MDC.put("notification.channel", channel);
        }
        if (status != null) {
            MDC.put("notification.status", status);
        }
    }

    public static void putDurationMs(long durationMs) {
        MDC.put("duration.ms", String.valueOf(durationMs));
    }

    public static void putBatchSize(int batchSize) {
        MDC.put("outbox.batch_size", String.valueOf(batchSize));
    }

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

    public static void putInstanceFields(String instanceId) {
        if (instanceId != null) {
            MDC.put("instance.id", instanceId);
        }
    }

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
