package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** In-memory queue, publisher worker, and recovery worker tuning. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OutboxProperties {

    /** Per-pod hot queue between PostgreSQL commit and Kafka publish. */
    @Builder.Default
    private MemoryQueueProperties memoryQueue = MemoryQueueProperties.builder().build();

    /** Background worker that claims rows and publishes batches to Kafka. */
    @Builder.Default
    private PublisherProperties publisher = PublisherProperties.builder().build();

    /** Scheduled worker that re-enqueues stuck outbox rows. */
    @Builder.Default
    private RecoveryProperties recovery = RecoveryProperties.builder().build();
}
