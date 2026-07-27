package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;

/** Bounded per-pod queue between DB commit and Kafka publish. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class MemoryQueueProperties {

    /** Maximum number of event ids held in memory before enqueue is rejected. */
    @Builder.Default
    private int capacity = 10000;

    /** Maximum ids drained into one publisher batch after the first poll. */
    @Builder.Default
    private int batchSize = 250;

    /** Time to wait for the first id before the publisher loop continues. */
    @Builder.Default
    private Duration batchWait = Duration.ofMillis(50);

    /** Queue fill ratio above which adaptive rate limiting starts throttling. */
    @Builder.Default
    private double usageThreshold = 0.8;
}
