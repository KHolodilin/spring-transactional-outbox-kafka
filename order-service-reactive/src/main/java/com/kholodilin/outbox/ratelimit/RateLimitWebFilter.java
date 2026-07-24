package com.kholodilin.outbox.ratelimit;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.RateLimitBucketProperties;
import com.kholodilin.outbox.logging.StructuredLogContext;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpRequestDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Bucket4j WebFilter rate limiting for order creation (global, per customer, per IP).
 */
@Component
@RequiredArgsConstructor
public class RateLimitWebFilter implements WebFilter, Ordered {

    private final AppProperties properties;
    private final InMemoryEventQueue eventQueue;
    private final OutboxMetrics metrics;
    private final ObjectMapper objectMapper;

    private Bucket globalBucket;
    private final Map<Long, Bucket> customerBuckets = new ConcurrentHashMap<>();
    private final Map<String, Bucket> ipBuckets = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        globalBucket = createBucket(properties.getRateLimit().getGlobal());
    }

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE + 10;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        ServerHttpRequest request = exchange.getRequest();
        if (!HttpMethod.POST.equals(request.getMethod())
                || !request.getPath().value().endsWith("/api/v1/orders")) {
            return chain.filter(exchange);
        }

        return DataBufferUtils.join(request.getBody())
                .defaultIfEmpty(exchange.getResponse().bufferFactory().wrap(new byte[0]))
                .flatMap(dataBuffer -> {
                    byte[] bytes = new byte[dataBuffer.readableByteCount()];
                    dataBuffer.read(bytes);
                    DataBufferUtils.release(dataBuffer);

                    double multiplier = throttleMultiplier();
                    if (!tryConsume(globalBucket, multiplier)) {
                        return reject(exchange);
                    }

                    String clientIp = resolveClientIp(request);
                    Bucket ipBucket = ipBuckets.computeIfAbsent(
                            clientIp, key -> createBucket(properties.getRateLimit().getPerIp()));
                    if (!tryConsume(ipBucket, multiplier)) {
                        return reject(exchange);
                    }

                    Long customerId = extractCustomerId(bytes);
                    if (customerId != null) {
                        Bucket customerBucket = customerBuckets.computeIfAbsent(
                                customerId, key -> createBucket(properties.getRateLimit().getPerCustomer()));
                        if (!tryConsume(customerBucket, multiplier)) {
                            return reject(exchange);
                        }
                    }

                    ServerHttpRequest decorated = new ServerHttpRequestDecorator(request) {
                        @Override
                        public Flux<DataBuffer> getBody() {
                            return Flux.defer(() -> {
                                DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(bytes);
                                return Flux.just(buffer);
                            });
                        }
                    };
                    return chain.filter(exchange.mutate().request(decorated).build());
                });
    }

    private double throttleMultiplier() {
        double threshold = properties.getOutbox().getMemoryQueue().getUsageThreshold();
        if (eventQueue.pressure() > threshold) {
            return properties.getRateLimit().getThrottleMultiplier();
        }
        return 1.0;
    }

    private boolean tryConsume(Bucket bucket, double multiplier) {
        long tokens = multiplier >= 1.0 ? 1L : Math.max(1L, Math.round(1.0 / multiplier));
        return bucket.tryConsume(tokens);
    }

    private Mono<Void> reject(ServerWebExchange exchange) {
        StructuredLogContext.putEventAction("http.request.rejected");
        metrics.incrementRateLimitRejects();
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().setContentType(MediaType.APPLICATION_PROBLEM_JSON);
        byte[] body = "{\"title\":\"Too Many Requests\",\"status\":429,\"detail\":\"Rate limit exceeded\"}"
                .getBytes(StandardCharsets.UTF_8);
        DataBuffer buffer = exchange.getResponse().bufferFactory().wrap(body);
        return exchange.getResponse().writeWith(Mono.just(buffer));
    }

    private Bucket createBucket(RateLimitBucketProperties limit) {
        Bandwidth bandwidth = Bandwidth.builder()
                .capacity(limit.getCapacity())
                .refillGreedy(limit.getRefillPerSecond(), Duration.ofSeconds(1))
                .build();
        return Bucket.builder().addLimit(bandwidth).build();
    }

    private String resolveClientIp(ServerHttpRequest request) {
        String forwarded = request.getHeaders().getFirst("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        if (request.getRemoteAddress() != null && request.getRemoteAddress().getAddress() != null) {
            return request.getRemoteAddress().getAddress().getHostAddress();
        }
        return "unknown";
    }

    private Long extractCustomerId(byte[] content) {
        if (content.length == 0) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(new String(content, StandardCharsets.UTF_8));
            JsonNode customerId = node.get("customerId");
            return customerId == null || customerId.isNull() ? null : customerId.asLong();
        } catch (Exception ex) {
            return null;
        }
    }
}
