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

    /** Whether the recovery scheduler is active. */
    @Builder.Default
    private boolean enabled = true;

    /** Delay between recovery scans after the previous run completes. */
    @Builder.Default
    private Duration interval = Duration.ofSeconds(10);

    /** Maximum number of outbox ids claimed and re-enqueued per scan. */
    @Builder.Default
    private int batchSize = 500;
}
