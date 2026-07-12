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

    @Builder.Default
    private MemoryQueueProperties memoryQueue = MemoryQueueProperties.builder().build();

    @Builder.Default
    private PublisherProperties publisher = PublisherProperties.builder().build();

    @Builder.Default
    private RecoveryProperties recovery = RecoveryProperties.builder().build();
}
