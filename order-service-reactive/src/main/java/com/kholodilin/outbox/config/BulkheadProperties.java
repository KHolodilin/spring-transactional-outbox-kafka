package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Create-order concurrency bulkhead so ingress cannot exhaust the R2DBC pool.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class BulkheadProperties {

    /**
     * Max simultaneous {@code POST /api/v1/orders} transactions.
     * Keep below the R2DBC pool size to leave headroom for publisher/recovery.
     */
    @Builder.Default
    private int maxConcurrentCreates = 55;
}
