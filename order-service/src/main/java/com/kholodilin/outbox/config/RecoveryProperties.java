package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Duration;

/** Scheduled scan of stuck ACTIVE rows — enqueue only, never publishes directly. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RecoveryProperties {

    @Builder.Default
    private boolean enabled = true;

    @Builder.Default
    private Duration interval = Duration.ofSeconds(10);

    @Builder.Default
    private int batchSize = 500;
}
