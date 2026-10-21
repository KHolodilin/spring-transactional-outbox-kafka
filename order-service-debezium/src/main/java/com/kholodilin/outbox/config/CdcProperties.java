package com.kholodilin.outbox.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Replication-slot signal used to tighten the HTTP rate limit.
 * <p>
 * Outbox rows are insert-only, so a row count is not a backlog. The backlog is how far
 * {@code confirmed_flush_lsn} lags {@code pg_current_wal_lsn}. {@code lagCapacityBytes} is the
 * byte analog of the servlet in-memory queue capacity (10 MiB ≈ 10000 events).
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CdcProperties {

    /** Replication slot created by the Debezium outbox connector. */
    @Builder.Default
    private String slotName = "debezium_outbox";

    /**
     * WAL lag, in bytes, that counts as pressure {@code 1.0}.
     * The rate limit starts spending extra tokens at {@code usage-threshold} of this capacity.
     */
    @Builder.Default
    private long lagCapacityBytes = 10_485_760L;
}
