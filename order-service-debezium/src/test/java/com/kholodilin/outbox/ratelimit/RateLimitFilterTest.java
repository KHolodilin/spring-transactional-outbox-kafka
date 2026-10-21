package com.kholodilin.outbox.ratelimit;

import tools.jackson.databind.json.JsonMapper;
import com.kholodilin.outbox.cdc.CdcSlotPressure;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.RateLimitBucketProperties;
import com.kholodilin.outbox.config.RateLimitProperties;
import com.kholodilin.outbox.metrics.OrderServiceMetrics;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RateLimitFilterTest {

    private SimpleMeterRegistry registry;
    private OrderServiceMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        metrics = new OrderServiceMetrics(registry);
        ReflectionTestUtils.invokeMethod(metrics, "registerMeters");
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void skipsNonOrderPostRequests() {
        RateLimitFilter filter = filter(10, 10, 10);
        assertThat(filter.shouldNotFilter(new MockHttpServletRequest("GET", "/api/v1/orders"))).isTrue();
        assertThat(filter.shouldNotFilter(new MockHttpServletRequest("POST", "/actuator/health"))).isTrue();
        assertThat(filter.shouldNotFilter(new MockHttpServletRequest("POST", "/api/v1/orders"))).isFalse();
    }

    @Test
    void rejectsWhenGlobalBucketExhausted() throws Exception {
        RateLimitFilter filter = filter(1, 10, 10);
        filter.doFilter(orderRequest(1L, null), new MockHttpServletResponse(), new MockFilterChain());

        MockHttpServletResponse rejected = new MockHttpServletResponse();
        filter.doFilter(orderRequest(2L, null), rejected, new MockFilterChain());

        assertThat(rejected.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(rejected.getContentAsString()).contains("Rate limit exceeded");
        assertThat(MDC.get("event.action")).isEqualTo("http.request.rejected");
        assertThat(registry.find("outbox.rate_limit.rejects").counter().count()).isEqualTo(1.0);
    }

    @Test
    void rejectsWhenIpBucketExhausted() throws Exception {
        RateLimitFilter filter = filter(10, 1, 10);
        filter.doFilter(orderRequest(1L, null), new MockHttpServletResponse(), new MockFilterChain());

        MockHttpServletResponse rejected = new MockHttpServletResponse();
        filter.doFilter(orderRequest(2L, null), rejected, new MockFilterChain());

        assertThat(rejected.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
    }

    @Test
    void rejectsWhenCustomerBucketExhausted() throws Exception {
        RateLimitFilter filter = filter(10, 10, 1);
        filter.doFilter(orderRequest(42L, null), new MockHttpServletResponse(), new MockFilterChain());

        MockHttpServletResponse rejected = new MockHttpServletResponse();
        filter.doFilter(orderRequest(42L, "203.0.113.9"), rejected, new MockFilterChain());

        assertThat(rejected.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
    }

    @Test
    void allowsRequestAndKeepsBodyReadable() throws Exception {
        RateLimitFilter filter = filter(10, 10, 10);
        MockHttpServletRequest request = orderRequest(42L, "203.0.113.1, 198.51.100.2");
        MockHttpServletResponse response = new MockHttpServletResponse();
        MockFilterChain chain = new MockFilterChain();

        filter.doFilter(request, response, chain);

        assertThat(response.getStatus()).isEqualTo(200);
        HttpServletRequest seen = (HttpServletRequest) chain.getRequest();
        assertThat(new String(seen.getInputStream().readAllBytes(), StandardCharsets.UTF_8)).contains("\"customerId\":42");
        CachedBodyHttpServletRequest wrapped = new CachedBodyHttpServletRequest(request, "{\"customerId\":7}".getBytes(StandardCharsets.UTF_8));
        assertThat(wrapped.getReader().readLine()).contains("customerId");
        wrapped.setCharacterEncoding("UTF-8");
        ServletInputStream stream = wrapped.getInputStream();
        assertThat(stream.isFinished()).isFalse();
        assertThat(stream.isReady()).isTrue();
        stream.setReadListener(null);
        assertThat(stream.read()).isGreaterThanOrEqualTo(0);
        while (stream.read() != -1) {
            // drain
        }
        assertThat(stream.isFinished()).isTrue();
        wrapped.setCharacterEncoding("UTF-8");
        assertThat(wrapped.getReader().readLine()).contains("customerId");
        assertThat(wrapped.getContentLength()).isEqualTo(wrapped.getContentLengthLong());
    }

    @Test
    void missingOrInvalidCustomerIdStillPasses() throws Exception {
        RateLimitFilter filter = filter(10, 10, 1);
        MockHttpServletRequest empty = new MockHttpServletRequest("POST", "/api/v1/orders");
        empty.setContent(new byte[0]);
        filter.doFilter(empty, new MockHttpServletResponse(), new MockFilterChain());

        MockHttpServletRequest missingCustomer = orderRequestBody("{}");
        filter.doFilter(missingCustomer, new MockHttpServletResponse(), new MockFilterChain());

        MockHttpServletRequest nullCustomer = orderRequestBody("{\"customerId\":null}");
        filter.doFilter(nullCustomer, new MockHttpServletResponse(), new MockFilterChain());

        MockHttpServletRequest broken = orderRequestBody("not-json");
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(broken, response, new MockFilterChain());

        assertThat(response.getStatus()).isEqualTo(200);
        MockHttpServletRequest blankForwarded = orderRequest(3L, "   ");
        filter.doFilter(blankForwarded, new MockHttpServletResponse(), new MockFilterChain());
    }

    @Test
    void highSlotLagSpendsTwoTokens() throws Exception {
        RateLimitFilter filter = filter(1, 10, 10, 0.9);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(orderRequest(1L, null), response, new MockFilterChain());

        assertThat(response.getStatus()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS.value());
        assertThat(registry.get("outbox.rate_limit.rejects").counter().count()).isEqualTo(1.0);
    }

    @Test
    void slotLagAtThresholdStillCostsOneToken() throws Exception {
        RateLimitFilter filter = filter(1, 10, 10, 0.8);
        MockHttpServletResponse response = new MockHttpServletResponse();

        filter.doFilter(orderRequest(1L, null), response, new MockFilterChain());

        assertThat(response.getStatus()).isEqualTo(200);
    }

    @Test
    void stricterMultiplierConsumesMoreThanOneToken() {
        RateLimitFilter filter = filter(10, 10, 10);
        Bucket bucket = Bucket.builder()
                .addLimit(Bandwidth.builder().capacity(1).refillGreedy(1, Duration.ofSeconds(1)).build())
                .build();

        Boolean consumed = ReflectionTestUtils.invokeMethod(filter, "tryConsume", bucket, 0.5);

        assertThat(consumed).isFalse();
    }

    private RateLimitFilter filter(long global, long perIp, long perCustomer) {
        return filter(global, perIp, perCustomer, 0.0);
    }

    private RateLimitFilter filter(long global, long perIp, long perCustomer, double slotPressure) {
        RateLimitProperties limits = RateLimitProperties.builder()
                .throttleMultiplier(0.5)
                .global(bucket(global))
                .perIp(bucket(perIp))
                .perCustomer(bucket(perCustomer))
                .build();
        CdcSlotPressure pressure = mock(CdcSlotPressure.class);
        when(pressure.pressure()).thenReturn(slotPressure);
        RateLimitFilter filter = new RateLimitFilter(
                AppProperties.builder().rateLimit(limits).build(),
                pressure,
                metrics,
                JsonMapper.builder().build());
        ReflectionTestUtils.invokeMethod(filter, "init");
        return filter;
    }

    private static RateLimitBucketProperties bucket(long capacity) {
        return RateLimitBucketProperties.builder().capacity(capacity).refillPerSecond(capacity).build();
    }

    private static MockHttpServletRequest orderRequest(long customerId, String forwardedFor) {
        MockHttpServletRequest request = orderRequestBody(
                "{\"customerId\":" + customerId + ",\"items\":[{\"productId\":\"p1\",\"quantity\":1,\"price\":1.0}]}");
        if (forwardedFor != null) {
            request.addHeader("X-Forwarded-For", forwardedFor);
        }
        return request;
    }

    private static MockHttpServletRequest orderRequestBody(String json) {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/orders");
        request.setContentType("application/json");
        request.setContent(json.getBytes(StandardCharsets.UTF_8));
        return request;
    }
}
