package com.kholodilin.outbox.cdc;

import com.kholodilin.outbox.config.AppProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Replication-slot lag as a 0..1 pressure value, sampled about once a second.
 * <p>
 * A missing slot or a failed query yields pressure {@code 0} so ingress is not throttled
 * when the lag signal itself is unavailable.
 */
@Slf4j
@Component
public class CdcSlotPressure {

    static final long REFRESH_INTERVAL_NANOS = 1_000_000_000L;

    private static final String LAG_SQL = """
            SELECT CASE
                       WHEN confirmed_flush_lsn IS NULL THEN 0
                       ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)
                   END
            FROM pg_replication_slots
            WHERE slot_name = ?
            """;

    private final JdbcTemplate jdbcTemplate;
    private final AppProperties properties;
    private final LongSupplier nanoTime;
    private final AtomicReference<Sample> sample = new AtomicReference<>(Sample.empty());

    @Autowired
    public CdcSlotPressure(JdbcTemplate jdbcTemplate, AppProperties properties) {
        this(jdbcTemplate, properties, System::nanoTime);
    }

    CdcSlotPressure(JdbcTemplate jdbcTemplate, AppProperties properties, LongSupplier nanoTime) {
        this.jdbcTemplate = jdbcTemplate;
        this.properties = properties;
        this.nanoTime = nanoTime;
    }

    /**
     * @return slot lag divided by {@code app.cdc.lag-capacity-bytes}, capped to {@code [0, 1]}
     */
    public double pressure() {
        long now = nanoTime.getAsLong();
        Sample current = sample.get();
        if (isFresh(current, now)) {
            return current.value;
        }
        return refresh(now);
    }

    private synchronized double refresh(long observedAtNanos) {
        long now = nanoTime.getAsLong();
        Sample current = sample.get();
        if (isFresh(current, observedAtNanos) || isFresh(current, now)) {
            return current.value;
        }
        double value = readPressure();
        sample.set(new Sample(true, now, value));
        return value;
    }

    private double readPressure() {
        try {
            Object lagValue = jdbcTemplate.queryForObject(
                    LAG_SQL,
                    Object.class,
                    properties.getCdc().getSlotName());
            if (!(lagValue instanceof Number lag)) {
                return 0.0;
            }
            long capacity = properties.getCdc().getLagCapacityBytes();
            if (capacity <= 0) {
                return 0.0;
            }
            return Math.clamp(lag.doubleValue() / capacity, 0.0, 1.0);
        } catch (DataAccessException ex) {
            log.debug("Replication slot lag is unavailable, pressure stays at 0", ex);
            return 0.0;
        }
    }

    private static boolean isFresh(Sample current, long now) {
        return current.present && now - current.takenAtNanos < REFRESH_INTERVAL_NANOS;
    }

    private record Sample(boolean present, long takenAtNanos, double value) {
        private static Sample empty() {
            return new Sample(false, 0L, 0.0);
        }
    }
}
