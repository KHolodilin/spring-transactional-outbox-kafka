package com.kholodilin.outbox.ratelimit;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.BulkheadProperties;
import com.kholodilin.outbox.metrics.OrderServiceMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class CreateBulkheadFilterTest {

    @Mock
    private FilterChain filterChain;

    private CreateBulkheadFilter filter;
    private SimpleMeterRegistry registry;
    private OrderServiceMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new OrderServiceMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        AppProperties properties = AppProperties.builder()
                .bulkhead(BulkheadProperties.builder().maxConcurrentCreates(1).build())
                .build();
        filter = new CreateBulkheadFilter(properties, metrics);
        ReflectionTestUtils.invokeMethod(filter, "init");
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void skipsNonOrderPostRequests() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/api/v1/orders");
        assertThat(filter.shouldNotFilter(request)).isTrue();
    }

    @Test
    void allowsRequestWhenPermitAvailable() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(orderRequest(), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(200);
        verify(filterChain).doFilter(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(response));
        assertThat(registry.find("order.bulkhead.in_flight").gauge().value()).isEqualTo(0.0);
    }

    @Test
    void rejectsWith429WhenNoPermitLeft() throws Exception {
        Semaphore permits = (Semaphore) ReflectionTestUtils.getField(filter, "createPermits");
        assertThat(permits.tryAcquire()).isTrue();

        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(orderRequest(), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(response.getContentAsString()).contains("Create concurrency limit exceeded");
        assertThat(MDC.get("event.action")).isEqualTo("http.request.rejected.bulkhead");
        assertThat(registry.find("order.bulkhead.rejects").counter().count()).isEqualTo(1.0);
        verifyNoInteractions(filterChain);

        permits.release();
    }

    @Test
    void releasesPermitAfterRequest() throws Exception {
        MockHttpServletResponse first = new MockHttpServletResponse();
        filter.doFilter(orderRequest(), first, filterChain);

        MockHttpServletResponse second = new MockHttpServletResponse();
        filter.doFilter(orderRequest(), second, filterChain);

        assertThat(second.getStatus()).isEqualTo(200);
        verify(filterChain, times(2)).doFilter(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    private static MockHttpServletRequest orderRequest() {
        return new MockHttpServletRequest("POST", "/api/v1/orders");
    }
}
