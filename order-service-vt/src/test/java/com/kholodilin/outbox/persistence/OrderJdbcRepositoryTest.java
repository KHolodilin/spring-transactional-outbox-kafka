package com.kholodilin.outbox.persistence;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderJdbcRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    void insertOrderReturnsGeneratedId() {
        OrderJdbcRepository repository = new OrderJdbcRepository(jdbcTemplate);
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class), any(), any(), any(), any(), any()))
                .thenReturn(101L);

        long orderId = repository.insertOrder(42L, BigDecimal.TEN, now);

        assertThat(orderId).isEqualTo(101L);
    }

    @Test
    void insertOrderItemWritesLineItem() {
        OrderJdbcRepository repository = new OrderJdbcRepository(jdbcTemplate);
        Instant now = Instant.parse("2026-01-01T00:00:00Z");

        repository.insertOrderItem(101L, 42L, "sku-1", 2, BigDecimal.ONE, now);

        verify(jdbcTemplate).update(anyString(), eq(101L), eq(42L), eq("sku-1"), eq(2), eq(BigDecimal.ONE), any());
    }
}
