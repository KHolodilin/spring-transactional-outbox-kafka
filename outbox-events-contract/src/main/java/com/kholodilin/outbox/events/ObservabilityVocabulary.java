package com.kholodilin.outbox.events;

import java.util.List;

/**
 * Shared {@code event.action} values and Tempo span names for servlet, VT, and reactive pipelines.
 */
public final class ObservabilityVocabulary {

    public static final String HTTP_REQUEST_ACCEPTED = "http.request.accepted";
    public static final String HTTP_REQUEST_COMPLETED = "http.request.completed";
    public static final String HTTP_REQUEST_REJECTED = "http.request.rejected";
    public static final String HTTP_REQUEST_REJECTED_BULKHEAD = "http.request.rejected.bulkhead";

    public static final String OUTBOX_EVENT_PERSISTED = "outbox.event.persisted";
    public static final String OUTBOX_BATCH_LOADED = "outbox.batch.loaded";
    public static final String OUTBOX_BATCH_PUBLISHED = "outbox.batch.published";
    public static final String OUTBOX_PUBLISH_FAILED = "outbox.publish.failed";
    public static final String OUTBOX_RETRY = "outbox.retry";
    public static final String OUTBOX_RECOVERY_COMPLETED = "outbox.recovery.completed";

    public static final String NOTIFICATION_BATCH_RECEIVED = "notification.batch.received";
    public static final String NOTIFICATION_PROCESSING_STARTED = "notification.processing.started";
    public static final String NOTIFICATION_PROCESSED = "notification.processed";
    public static final String NOTIFICATION_DUPLICATE_SKIPPED = "notification.duplicate.skipped";
    public static final String NOTIFICATION_CONFLICT_SKIPPED = "notification.conflict.skipped";
    public static final String NOTIFICATION_PROCESSING_FAILED = "notification.processing.failed";

    public static final String SPAN_OUTBOX_SAVE = "outbox.save";
    public static final String SPAN_BATCH_FETCH = "batch.fetch";
    public static final String SPAN_BATCH_PUBLISH = "batch.publish";
    public static final String SPAN_BATCH_COMPLETE = "batch.complete";
    public static final String SPAN_OUTBOX_PUBLISH = "outbox.publish";
    public static final String SPAN_NOTIFICATION_BATCH_RECEIVE = "notification.batch.receive";
    public static final String SPAN_NOTIFICATION_CONSUME = "notification.consume";

    public static final List<String> ORDER_EVENT_ACTIONS = List.of(
            HTTP_REQUEST_ACCEPTED,
            HTTP_REQUEST_COMPLETED,
            HTTP_REQUEST_REJECTED,
            HTTP_REQUEST_REJECTED_BULKHEAD,
            OUTBOX_EVENT_PERSISTED,
            OUTBOX_BATCH_LOADED,
            OUTBOX_BATCH_PUBLISHED,
            OUTBOX_PUBLISH_FAILED,
            OUTBOX_RETRY,
            OUTBOX_RECOVERY_COMPLETED
    );

    public static final List<String> STUB_EVENT_ACTIONS = List.of(
            NOTIFICATION_BATCH_RECEIVED,
            NOTIFICATION_PROCESSING_STARTED,
            NOTIFICATION_PROCESSED,
            NOTIFICATION_DUPLICATE_SKIPPED,
            NOTIFICATION_CONFLICT_SKIPPED,
            NOTIFICATION_PROCESSING_FAILED
    );

    public static final List<String> ORDER_SPAN_NAMES = List.of(
            SPAN_OUTBOX_SAVE,
            SPAN_BATCH_FETCH,
            SPAN_BATCH_PUBLISH,
            SPAN_BATCH_COMPLETE,
            SPAN_OUTBOX_PUBLISH
    );

    public static final List<String> STUB_SPAN_NAMES = List.of(
            SPAN_NOTIFICATION_BATCH_RECEIVE,
            SPAN_NOTIFICATION_CONSUME
    );

    public static final List<String> ORDER_SOURCE_TOKENS = List.of(
            "HTTP_REQUEST_ACCEPTED",
            "HTTP_REQUEST_COMPLETED",
            "HTTP_REQUEST_REJECTED",
            "HTTP_REQUEST_REJECTED_BULKHEAD",
            "OUTBOX_EVENT_PERSISTED",
            "OUTBOX_BATCH_LOADED",
            "OUTBOX_BATCH_PUBLISHED",
            "OUTBOX_PUBLISH_FAILED",
            "OUTBOX_RETRY",
            "OUTBOX_RECOVERY_COMPLETED",
            "SPAN_OUTBOX_SAVE",
            "SPAN_BATCH_FETCH",
            "SPAN_BATCH_PUBLISH",
            "SPAN_BATCH_COMPLETE",
            "SPAN_OUTBOX_PUBLISH"
    );

    public static final List<String> STUB_SOURCE_TOKENS = List.of(
            "NOTIFICATION_BATCH_RECEIVED",
            "NOTIFICATION_PROCESSING_STARTED",
            "NOTIFICATION_PROCESSED",
            "NOTIFICATION_DUPLICATE_SKIPPED",
            "NOTIFICATION_CONFLICT_SKIPPED",
            "NOTIFICATION_PROCESSING_FAILED",
            "SPAN_NOTIFICATION_BATCH_RECEIVE",
            "SPAN_NOTIFICATION_CONSUME"
    );

    private ObservabilityVocabulary() {
    }
}
