package com.kholodilin.outbox.logging;

import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HttpRequestMdcFilterTest {

    private final HttpRequestMdcFilter filter = new HttpRequestMdcFilter();

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void clearsRequestMdcAfterOrderPost() throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest("POST", "/api/v1/orders");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        MDC.put("correlationId", "corr-1");
        MDC.put("customerId", "42");

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("customerId")).isNull();
    }

    @Test
    void doesNotFilterOtherPaths() {
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        assertThat(filter.shouldNotFilter(request)).isTrue();
    }
}
