package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Token-bucket limits with adaptive backpressure when queue pressure is high. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitProperties {

    /** Multiplier applied to bucket refill rates while the queue is under pressure. */
    @Builder.Default
    private double throttleMultiplier = 0.5;

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
