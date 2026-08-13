package com.kholodilin.outbox.ratelimit;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.BulkheadProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.Semaphore;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CreateBulkheadWebFilterTest {

    @Mock
    private WebFilterChain chain;

    private CreateBulkheadWebFilter filter;
    private SimpleMeterRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        OutboxMetrics metrics = new OutboxMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        AppProperties properties = AppProperties.builder()
                .bulkhead(BulkheadProperties.builder().maxConcurrentCreates(1).build())
                .build();
        filter = new CreateBulkheadWebFilter(properties, metrics);
        ReflectionTestUtils.invokeMethod(filter, "init");
        org.mockito.Mockito.lenient().when(chain.filter(any())).thenReturn(Mono.empty());
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void getOrderIsHighestPrecedencePlusTwenty() {
        assertThat(filter.getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE + 20);
    }

    @Test
    void passesThroughNonPostRequests() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/orders").build());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
    }

    @Test
    void allowsRequestWhenPermitAvailable() {
        MockServerWebExchange exchange = MockServerWebExchange.from(orderRequest());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
        assertThat(registry.find("order.bulkhead.in_flight").gauge().value()).isEqualTo(0.0);
    }

    @Test
    void rejectsWith429WhenNoPermitLeft() {
        Semaphore permits = (Semaphore) ReflectionTestUtils.getField(filter, "createPermits");
        assertThat(permits.tryAcquire()).isTrue();

        MockServerWebExchange exchange = MockServerWebExchange.from(orderRequest());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(MDC.get("event.action")).isEqualTo("http.request.rejected.bulkhead");
        assertThat(registry.find("order.bulkhead.rejects").counter().count()).isEqualTo(1.0);
        verifyNoInteractions(chain);

        permits.release();
    }

    @Test
    void releasesPermitAfterRequestCompletes() {
        MockServerWebExchange first = MockServerWebExchange.from(orderRequest());
        StepVerifier.create(filter.filter(first, chain)).verifyComplete();

        MockServerWebExchange second = MockServerWebExchange.from(orderRequest());
        StepVerifier.create(filter.filter(second, chain)).verifyComplete();

        verify(chain, times(2)).filter(any());
    }

    @Test
    void releasesPermitAfterChainError() {
        when(chain.filter(any())).thenReturn(Mono.error(new IllegalStateException("boom")));

        MockServerWebExchange first = MockServerWebExchange.from(orderRequest());
        StepVerifier.create(filter.filter(first, chain))
                .expectError(IllegalStateException.class)
                .verify();

        when(chain.filter(any())).thenReturn(Mono.empty());
        MockServerWebExchange second = MockServerWebExchange.from(orderRequest());
        StepVerifier.create(filter.filter(second, chain)).verifyComplete();

        verify(chain, times(2)).filter(any());
    }

    private static MockServerHttpRequest orderRequest() {
        return MockServerHttpRequest.post("/api/v1/orders").build();
    }
}
