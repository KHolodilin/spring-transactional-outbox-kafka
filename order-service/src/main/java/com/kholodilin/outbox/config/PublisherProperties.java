package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;

/** Batch publisher lease, retry, and poll interval. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PublisherProperties {

    @Builder.Default
    private Duration leaseDuration = Duration.ofSeconds(30);

    @Builder.Default
    private int maxRetries = 5;

    @Builder.Default
    private Duration pollInterval = Duration.ofMillis(100);
}
