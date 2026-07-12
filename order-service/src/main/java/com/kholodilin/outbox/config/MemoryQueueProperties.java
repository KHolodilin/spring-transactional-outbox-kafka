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

    @Builder.Default
    private int capacity = 10000;

    @Builder.Default
    private int batchSize = 100;

    @Builder.Default
    private Duration batchWait = Duration.ofMillis(50);

    @Builder.Default
    private double usageThreshold = 0.8;
}
