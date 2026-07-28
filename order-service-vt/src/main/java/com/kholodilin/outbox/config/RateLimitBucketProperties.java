package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** Token-bucket capacity and refill rate for rate limiting. */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitBucketProperties {

    /** Maximum number of tokens the bucket can hold (burst size). */
    private long capacity;

    /** Tokens added to the bucket per second under normal conditions. */
    private long refillPerSecond;
}
