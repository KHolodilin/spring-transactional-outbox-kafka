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

import jakarta.servlet.http.HttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@ExtendWith(MockitoExtension.class)
class RateLimitFilterTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @Mock
    private FilterChain filterChain;

    @Mock
    private Bucket globalBucket;

    private RateLimitFilter filter;
    private OutboxMetrics metrics;

    @BeforeEach
    void setUp() {
        metrics = new OutboxMetrics(new SimpleMeterRegistry());
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
        filter = new RateLimitFilter(strictProperties(), eventQueue, metrics, JsonMapper.builder().build());
        ReflectionTestUtils.setField(filter, "globalBucket", globalBucket);
        lenient().when(eventQueue.pressure()).thenReturn(0.0);
        lenient().when(globalBucket.tryConsume(anyLong())).thenReturn(true);
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
    void rejectsWhenGlobalBucketExhausted() throws Exception {
        when(globalBucket.tryConsume(anyLong())).thenReturn(false);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(orderRequest(42L), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(MDC.get("event.action")).isEqualTo("http.request.rejected");
        verifyNoInteractions(filterChain);
    }

    @Test
    void allowsRequestWhenBucketsHaveCapacity() throws Exception {
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(orderRequest(42L), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(200);
        verify(filterChain).doFilter(argThat(req -> {
            try {
                byte[] body = ((HttpServletRequest) req).getInputStream().readAllBytes();
                return new String(body, StandardCharsets.UTF_8).contains("\"customerId\":42");
            } catch (Exception ex) {
                return false;
            }
        }), eq(response));
    }

    @Test
    void appliesAdaptiveThrottleWhenQueuePressureHigh() throws Exception {
        when(eventQueue.pressure()).thenReturn(0.9);
        when(globalBucket.tryConsume(2L)).thenReturn(false);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(orderRequest(99L), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
    }

    @Test
    void resolvesForwardedClientIp() throws Exception {
        MockHttpServletRequest request = orderRequest(1L);
        request.addHeader("X-Forwarded-For", "203.0.113.1, 198.51.100.2");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, filterChain);

        assertThat(response.getStatus()).isEqualTo(200);
        verify(filterChain).doFilter(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.eq(response));
    }

    @Test
    void extractsCustomerIdFromRequestBody() {
        Long customerId = ReflectionTestUtils.invokeMethod(
                filter,
                "extractCustomerId",
                (Object) "{\"customerId\":77,\"items\":[]}".getBytes(StandardCharsets.UTF_8));

        assertThat(customerId).isEqualTo(77L);
    }

    @Test
    void rejectsWhenPerIpBucketExhausted() throws Exception {
        Bucket ipBucket = mock(Bucket.class);
        when(ipBucket.tryConsume(1L)).thenReturn(false);
        @SuppressWarnings("unchecked")
        Map<String, Bucket> ipBuckets = (Map<String, Bucket>) ReflectionTestUtils.getField(filter, "ipBuckets");
        ipBuckets.put("127.0.0.1", ipBucket);

        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(orderRequest(42L), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        verifyNoInteractions(filterChain);
    }

    @Test
    void rejectsWhenPerCustomerBucketExhausted() throws Exception {
        Bucket customerBucket = mock(Bucket.class);
        when(customerBucket.tryConsume(1L)).thenReturn(false);
        @SuppressWarnings("unchecked")
        Map<Long, Bucket> customerBuckets = (Map<Long, Bucket>) ReflectionTestUtils.getField(filter, "customerBuckets");
        customerBuckets.put(42L, customerBucket);

        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(orderRequest(42L), response, filterChain);

        assertThat(response.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        verifyNoInteractions(filterChain);
    }

    @Test
    void extractCustomerIdReturnsNullForEmptyOrInvalidBody() {
        assertThat((Long) ReflectionTestUtils.invokeMethod(filter, "extractCustomerId", (Object) new byte[0])).isNull();
        assertThat((Long) ReflectionTestUtils.invokeMethod(
                filter, "extractCustomerId", (Object) "{not-json".getBytes(StandardCharsets.UTF_8))).isNull();
        assertThat((Long) ReflectionTestUtils.invokeMethod(
                filter, "extractCustomerId", (Object) "{\"items\":[]}".getBytes(StandardCharsets.UTF_8))).isNull();
    }

    @Test
    void usesRemoteAddrWhenForwardedHeaderBlank() throws Exception {
        MockHttpServletRequest request = orderRequest(1L);
        request.addHeader("X-Forwarded-For", "   ");
        request.setRemoteAddr("10.0.0.8");
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(request, response, filterChain);

        assertThat(response.getStatus()).isEqualTo(200);
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

    private static MockHttpServletRequest orderRequest(long customerId) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/orders");
        request.setContentType("application/json");
        request.setContent(("{\"customerId\":" + customerId + ",\"items\":[{\"productId\":\"p1\",\"quantity\":1,\"price\":1.0}]}")
                .getBytes());
        return request;
    }
}
