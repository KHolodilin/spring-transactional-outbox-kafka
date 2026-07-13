package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.AppProperties;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;

/** Adds instance and tracing alias fields to MDC for every HTTP request. */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@RequiredArgsConstructor
public class LogMdcEnricherFilter extends OncePerRequestFilter {

    private final AppProperties appProperties;

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        StructuredLogContext.putInstanceFields(appProperties.getInstanceId());
        StructuredLogContext.enrichTracingAliases();
        try {
            filterChain.doFilter(request, response);
        } finally {
            StructuredLogContext.enrichTracingAliases();
        }
    }
}
