package com.kholodilin.outbox.ratelimit;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.MemoryQueueProperties;
import com.kholodilin.outbox.config.OutboxProperties;
import com.kholodilin.outbox.config.RateLimitBucketProperties;
import com.kholodilin.outbox.config.RateLimitProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import io.github.bucket4j.Bucket;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RateLimitWebFilterTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private WebFilterChain chain;

    @Mock
    private Bucket globalBucket;

    private RateLimitWebFilter filter;
    private OutboxMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new OutboxMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        filter = new RateLimitWebFilter(strictProperties(), eventQueue, metrics, JsonMapper.builder().build());
        ReflectionTestUtils.setField(filter, "globalBucket", globalBucket);
        lenient().when(eventQueue.pressure()).thenReturn(0.0);
        lenient().when(globalBucket.tryConsume(anyLong())).thenReturn(true);
        lenient().when(chain.filter(any())).thenReturn(Mono.empty());
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void getOrderIsHighestPrecedencePlusTen() {
        assertThat(filter.getOrder()).isEqualTo(Ordered.HIGHEST_PRECEDENCE + 10);
    }

    @Test
    void passesThroughNonPostOrWrongPath() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/api/v1/orders").build());

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
        verifyNoInteractions(globalBucket);
    }

    @Test
    void rejectsWhenGlobalBucketExhausted() {
        when(globalBucket.tryConsume(anyLong())).thenReturn(false);
        MockServerWebExchange exchange = MockServerWebExchange.from(orderRequest(42L));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(MDC.get("event.action")).isEqualTo("http.request.rejected");
        verifyNoInteractions(chain);
    }

    @Test
    void allowsRequestWhenBucketsHaveCapacity() {
        MockServerWebExchange exchange = MockServerWebExchange.from(orderRequest(42L));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(any());
    }

    @Test
    void appliesAdaptiveThrottleWhenQueuePressureHigh() {
        when(eventQueue.pressure()).thenReturn(0.9);
        when(globalBucket.tryConsume(2L)).thenReturn(false);
        MockServerWebExchange exchange = MockServerWebExchange.from(orderRequest(99L));

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void resolvesForwardedClientIp() {
        MockServerHttpRequest request = MockServerHttpRequest
                .post("/api/v1/orders")
                .header("Content-Type", "application/json")
                .header("X-Forwarded-For", "203.0.113.1, 198.51.100.2")
                .body("{\"customerId\":1,\"items\":[{\"productId\":\"p1\",\"quantity\":1,\"price\":1.0}]}");
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(any());
    }

    private static AppProperties strictProperties() {
        RateLimitBucketProperties one = RateLimitBucketProperties.builder().capacity(1).refillPerSecond(1).build();
        return AppProperties.builder()
                .rateLimit(RateLimitProperties.builder()
                        .global(one)
                        .perIp(one)
                        .perCustomer(one)
                        .throttleMultiplier(0.5)
                        .build())
                .outbox(OutboxProperties.builder()
                        .memoryQueue(MemoryQueueProperties.builder().usageThreshold(0.5).build())
                        .build())
                .build();
    }

    private static MockServerHttpRequest orderRequest(long customerId) {
        return MockServerHttpRequest
                .post("/api/v1/orders")
                .header("Content-Type", "application/json")
                .body("{\"customerId\":" + customerId
                        + ",\"items\":[{\"productId\":\"p1\",\"quantity\":1,\"price\":1.0}]}");
    }
}
