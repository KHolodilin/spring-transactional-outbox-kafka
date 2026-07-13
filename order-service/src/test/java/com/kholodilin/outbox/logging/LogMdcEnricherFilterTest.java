package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.AppProperties;
import jakarta.servlet.FilterChain;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class LogMdcEnricherFilterTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void enrichesInstanceMdcForEveryRequest() throws Exception {
        LogMdcEnricherFilter filter = new LogMdcEnricherFilter(AppProperties.builder().instanceId("pod-42").build());
        MockHttpServletRequest request = new MockHttpServletRequest("GET", "/actuator/health");
        MockHttpServletResponse response = new MockHttpServletResponse();
        FilterChain chain = mock(FilterChain.class);

        filter.doFilter(request, response, chain);

        verify(chain).doFilter(request, response);
        assertThat(MDC.get("instance.id")).isEqualTo("pod-42");
    }
}
