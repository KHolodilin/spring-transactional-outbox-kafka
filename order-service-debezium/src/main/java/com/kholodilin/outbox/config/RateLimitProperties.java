package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Token-bucket limits and the adaptive throttle applied when CDC lag is high. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitProperties {

    /**
     * Token-cost multiplier when replication-slot pressure is above {@link #usageThreshold}.
     * {@code 0.5} spends two tokens per request.
     */
    @Builder.Default
    private double throttleMultiplier = 0.5;

    /**
     * Slot-lag pressure above which {@link #throttleMultiplier} replaces a cost of one token.
     * Matches the servlet queue {@code usage-threshold}.
     */
    @Builder.Default
    private double usageThreshold = 0.8;

    /** Cluster-wide request rate limit. */
    @Builder.Default
    private RateLimitBucketProperties global = RateLimitBucketProperties.builder().capacity(1000).refillPerSecond(100).build();

    /** Per-customer request rate limit. */
    @Builder.Default
    private RateLimitBucketProperties perCustomer = RateLimitBucketProperties.builder().capacity(100).refillPerSecond(10).build();

    /** Per client IP request rate limit. */
    @Builder.Default
    private RateLimitBucketProperties perIp = RateLimitBucketProperties.builder().capacity(50).refillPerSecond(5).build();
}
