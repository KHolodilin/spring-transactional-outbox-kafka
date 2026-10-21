package com.kholodilin.outbox.cdc;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.CdcProperties;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CdcSlotPressureTest {

    private final JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);

    @Test
    void springConstructorReadsLag() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(0L);

        assertThat(new CdcSlotPressure(jdbcTemplate, properties(10_485_760L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void pressureIsLagDividedByCapacity() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(9_000_000L);

        double pressure = probe(properties(10_485_760L), new AtomicLong(1_000L)).pressure();

        assertThat(pressure).isCloseTo(9_000_000d / 10_485_760d, within(0.000_001));
    }

    @Test
    void pressureIsCappedAtOne() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(20_000_000L);

        assertThat(probe(properties(10_485_760L), new AtomicLong(1_000L)).pressure()).isEqualTo(1.0);
    }

    @Test
    void negativeLagIsZeroPressure() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(-1L);

        assertThat(probe(properties(10_485_760L), new AtomicLong(1_000L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void nullLagIsZeroPressure() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(null);

        assertThat(probe(properties(10_485_760L), new AtomicLong(1_000L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void nonNumericLagIsZeroPressure() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn("not-a-number");

        assertThat(probe(properties(10_485_760L), new AtomicLong(1_000L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void zeroCapacityIsZeroPressure() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(9_000_000L);

        assertThat(probe(properties(0L), new AtomicLong(1_000L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void missingSlotIsZeroPressure() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenThrow(new EmptyResultDataAccessException(1));

        assertThat(probe(properties(10_485_760L), new AtomicLong(1_000L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void queryFailureIsZeroPressure() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("custom_slot")))
                .thenThrow(new DataRetrievalFailureException("db down"));

        AppProperties properties = AppProperties.builder()
                .cdc(CdcProperties.builder().slotName("custom_slot").lagCapacityBytes(10_485_760L).build())
                .build();

        assertThat(probe(properties, new AtomicLong(1_000L)).pressure()).isEqualTo(0.0);
    }

    @Test
    void sampleIsReusedUntilTheRefreshIntervalElapses() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(9_000_000L, 1L);
        AtomicLong clock = new AtomicLong(5_000_000_000L);
        CdcSlotPressure pressure = probe(properties(10_485_760L), clock);

        double first = pressure.pressure();
        double cached = pressure.pressure();
        clock.addAndGet(CdcSlotPressure.REFRESH_INTERVAL_NANOS);
        double refreshed = pressure.pressure();

        assertThat(cached).isEqualTo(first);
        assertThat(refreshed).isLessThan(first);
        verify(jdbcTemplate, times(2)).queryForObject(anyString(), eq(Object.class), eq("debezium_outbox"));
    }

    @Test
    void refreshSkipsQueryWhenTheObservedSampleIsStillFresh() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(9_000_000L);
        AtomicLong clock = new AtomicLong(5_000_000_000L);
        CdcSlotPressure pressure = probe(properties(10_485_760L), clock);
        pressure.pressure();

        Double raced = ReflectionTestUtils.invokeMethod(pressure, "refresh", clock.get() + 10L);

        assertThat(raced).isEqualTo(pressure.pressure());
        verify(jdbcTemplate, times(1)).queryForObject(anyString(), eq(Object.class), eq("debezium_outbox"));
    }

    @Test
    void refreshSkipsQueryWhenTheClockMovedButTheSampleIsStillFresh() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Object.class), eq("debezium_outbox")))
                .thenReturn(9_000_000L);
        AtomicLong clock = new AtomicLong(5_000_000_000L);
        CdcSlotPressure pressure = probe(properties(10_485_760L), clock);
        pressure.pressure();

        Double raced = ReflectionTestUtils.invokeMethod(
                pressure,
                "refresh",
                clock.get() + CdcSlotPressure.REFRESH_INTERVAL_NANOS + 1L);

        assertThat(raced).isEqualTo(pressure.pressure());
        verify(jdbcTemplate, times(1)).queryForObject(anyString(), eq(Object.class), eq("debezium_outbox"));
    }

    private CdcSlotPressure probe(AppProperties properties, AtomicLong clock) {
        return new CdcSlotPressure(jdbcTemplate, properties, clock::get);
    }

    private static AppProperties properties(long lagCapacityBytes) {
        return AppProperties.builder()
                .cdc(CdcProperties.builder().lagCapacityBytes(lagCapacityBytes).build())
                .build();
    }
}
