package com.kholodilin.outbox.ratelimit;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.Semaphore;

/**
 * Create-order concurrency bulkhead so ingress cannot exhaust the R2DBC pool.
 * <p>
 * Runs after {@link RateLimitWebFilter}. Failed {@code tryAcquire} returns HTTP 429 immediately
 * (no wait on R2DBC {@code max-acquire-time}).
 */
@Component
@RequiredArgsConstructor
public class CreateBulkheadWebFilter implements WebFilter, Ordered {

    private final AppProperties properties;
    private final OutboxMetrics metrics;

    private Semaphore createPermits;
    private int maxConcurrentCreates;

    @PostConstruct
    void init() {
        maxConcurrentCreates = Math.max(1, properties.getBulkhead().getMaxConcurrentCreates());
        createPermits = new Semaphore(maxConcurrentCreates, true);
        metrics.updateBulkheadInFlight(0);
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 20;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        if (!HttpMethod.POST.equals(request.getMethod())
                || !request.getPath().value().endsWith("/api/v1/orders")) {
            return chain.filter(exchange);
        }

        if (!createPermits.tryAcquire()) {
            return reject(exchange);
        }
        metrics.updateBulkheadInFlight(maxConcurrentCreates - createPermits.availablePermits());
        return chain.filter(exchange)
                .doFinally(signal -> {
                    createPermits.release();
                    metrics.updateBulkheadInFlight(maxConcurrentCreates - createPermits.availablePermits());
                });
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        StructuredLogContext.putEventAction("http.request.rejected.bulkhead");
        metrics.incrementBulkheadRejects();
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        byte[] body = "{\"title\":\"Too Many Requests\",\"status\":429,\"detail\":\"Create concurrency limit exceeded\"}"
                .getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }
}
