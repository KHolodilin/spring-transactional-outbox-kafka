package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;

/** Batch publisher lease and retry settings. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PublisherProperties {

    /** How long a pod keeps an exclusive lease on claimed outbox rows. */
    @Builder.Default
    private Duration leaseDuration = Duration.ofSeconds(30);

    /** Failed publish attempts after which a row is moved to {@code DEAD}. */
    @Builder.Default
    private int maxRetries = 5;
}
