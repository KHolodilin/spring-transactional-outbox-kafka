package com.kholodilin.outbox.logging;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.core.Ordered;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HttpRequestMdcWebFilterTest {

    private final HttpRequestMdcWebFilter filter = new HttpRequestMdcWebFilter();

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void clearsRequestMdcAfterOrderPost() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/orders").build());
        WebFilterChain chain = mock(WebFilterChain.class);
        when(chain.filter(any())).thenAnswer(invocation -> {
            MDC.put("correlationId", "corr-1");
            MDC.put("customerId", "42");
            return Mono.empty();
        });

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        verify(chain).filter(exchange);
        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("customerId")).isNull();
        assertThat(filter.getOrder()).isEqualTo(Ordered.LOWEST_PRECEDENCE);
    }

    @Test
    void doesNotClearMdcForOtherPaths() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/actuator/health").build());
        WebFilterChain chain = mock(WebFilterChain.class);
        when(chain.filter(any())).thenAnswer(invocation -> {
            MDC.put("correlationId", "corr-1");
            return Mono.empty();
        });

        StepVerifier.create(filter.filter(exchange, chain)).verifyComplete();

        assertThat(MDC.get("correlationId")).isEqualTo("corr-1");
    }
}
