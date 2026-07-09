package com.kholodilin.outbox.config;

import java.time.Duration;

/** Batch publisher lease, retry, and poll interval. */
public class PublisherProperties {

    private Duration leaseDuration = Duration.ofSeconds(30);
    private int maxRetries = 5;
    private Duration pollInterval = Duration.ofMillis(100);

    public Duration getLeaseDuration() {
        return leaseDuration;
    }

    public void setLeaseDuration(Duration leaseDuration) {
        this.leaseDuration = leaseDuration;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }

    public Duration getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(Duration pollInterval) {
        this.pollInterval = pollInterval;
    }
}
