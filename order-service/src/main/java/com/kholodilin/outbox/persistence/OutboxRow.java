package com.kholodilin.outbox.persistence;

import com.kholodilin.outbox.events.OutboxStatus;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

/** Lightweight row loaded for publishing — payload kept as JSON string until send time. */
@Getter
@Builder
@AllArgsConstructor
public class OutboxRow {

    private final Long id;
    private final Long orderId;
    private final Long customerId;
    private final String eventType;
    private final String payload;
    private final OutboxStatus status;
    private final int retryCount;
}
