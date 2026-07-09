package com.kholodilin.outbox.config;

import java.time.Duration;

/** Scheduled scan of stuck ACTIVE rows — enqueue only, never publishes directly. */
public class RecoveryProperties {

    private boolean enabled = true;
    private Duration interval = Duration.ofSeconds(10);
    private int batchSize = 500;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public Duration getInterval() {
        return interval;
    }

    public void setInterval(Duration interval) {
        this.interval = interval;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}
