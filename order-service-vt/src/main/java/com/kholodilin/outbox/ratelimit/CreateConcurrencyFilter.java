package com.kholodilin.outbox.ratelimit;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OrderServiceMetrics;
import jakarta.annotation.PostConstruct;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.util.concurrent.Semaphore;

/**
 * Limits concurrent create-order transactions so ingress cannot exhaust the JDBC pool.
 * <p>
 * Runs after {@link RateLimitFilter}. Failed {@code tryAcquire} returns HTTP 429 immediately
 * (no wait on Hikari {@code connection-timeout}).
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 50)
@RequiredArgsConstructor
public class CreateConcurrencyFilter extends OncePerRequestFilter {

    private final AppProperties properties;
    private final OrderServiceMetrics metrics;

    private Semaphore createPermits;
    private int maxConcurrentCreates;

    @PostConstruct
    void init() {
        maxConcurrentCreates = Math.max(1, properties.getBackpressure().getMaxConcurrentCreates());
        createPermits = new Semaphore(maxConcurrentCreates, true);
        metrics.updateBackpressureInFlight(0);
    }

    /**
     * @return {@code true} unless this is {@code POST /api/v1/orders}
     */
    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equalsIgnoreCase(request.getMethod()) || !request.getRequestURI().endsWith("/api/v1/orders");
    }

    /**
     * Acquires a create permit for the duration of the request; rejects with 429 when none left.
     */
    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        if (!createPermits.tryAcquire()) {
            reject(response);
            return;
        }
        metrics.updateBackpressureInFlight(maxConcurrentCreates - createPermits.availablePermits());
        try {
            filterChain.doFilter(request, response);
        } finally {
            createPermits.release();
            metrics.updateBackpressureInFlight(maxConcurrentCreates - createPermits.availablePermits());
        }
    }

    private void reject(HttpServletResponse response) throws IOException {
        StructuredLogContext.putEventAction("http.request.rejected.backpressure");
        metrics.incrementBackpressureRejects();
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        response.getWriter().write(
                "{\"title\":\"Too Many Requests\",\"status\":429,\"detail\":\"Create concurrency limit exceeded\"}");
    }
}
