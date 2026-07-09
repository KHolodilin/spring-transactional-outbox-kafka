package com.kholodilin.outbox.ratelimit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.config.RateLimitBucketProperties;
import com.kholodilin.outbox.metrics.OutboxMetrics;
import com.kholodilin.outbox.queue.InMemoryEventQueue;
import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import org.springframework.web.util.ContentCachingRequestWrapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Token-bucket rate limiting for order creation (global, per customer, per IP).
 * <p>
 * When in-memory queue pressure exceeds the configured threshold, consumption cost increases
 * (adaptive backpressure) to slow down ingress while the publisher catches up.
 */
@Component
public class RateLimitFilter extends OncePerRequestFilter {

    private final Bucket globalBucket;
    private final Map<Long, Bucket> customerBuckets = new ConcurrentHashMap<>();
    private final Map<String, Bucket> ipBuckets = new ConcurrentHashMap<>();
    private final AppProperties properties;
    private final InMemoryEventQueue eventQueue;
    private final OutboxMetrics metrics;
    private final ObjectMapper objectMapper;

    public RateLimitFilter(
            AppProperties properties,
            InMemoryEventQueue eventQueue,
            OutboxMetrics metrics,
            ObjectMapper objectMapper
    ) {
        this.properties = properties;
        this.eventQueue = eventQueue;
        this.metrics = metrics;
        this.objectMapper = objectMapper;
        this.globalBucket = createBucket(properties.getRateLimit().getGlobal());
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return !"POST".equalsIgnoreCase(request.getMethod()) || !request.getRequestURI().endsWith("/api/v1/orders");
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain)
            throws ServletException, IOException {
        ContentCachingRequestWrapper wrapped = new ContentCachingRequestWrapper(request);
        double multiplier = throttleMultiplier();

        if (!tryConsume(globalBucket, multiplier)) {
            reject(response);
            return;
        }

        String clientIp = resolveClientIp(wrapped);
        Bucket ipBucket = ipBuckets.computeIfAbsent(clientIp, key -> createBucket(properties.getRateLimit().getPerIp()));
        if (!tryConsume(ipBucket, multiplier)) {
            reject(response);
            return;
        }

        Long customerId = extractCustomerId(wrapped);
        if (customerId != null) {
            Bucket customerBucket = customerBuckets.computeIfAbsent(customerId, key -> createBucket(properties.getRateLimit().getPerCustomer()));
            if (!tryConsume(customerBucket, multiplier)) {
                reject(response);
                return;
            }
        }

        filterChain.doFilter(wrapped, response);
    }

    private double throttleMultiplier() {
        double threshold = properties.getOutbox().getMemoryQueue().getUsageThreshold();
        if (eventQueue.pressure() > threshold) {
            return properties.getRateLimit().getThrottleMultiplier();
        }
        return 1.0;
    }

    /** Lower multiplier means more tokens consumed per request → stricter effective limit. */
    private boolean tryConsume(Bucket bucket, double multiplier) {
        long tokens = multiplier >= 1.0 ? 1L : Math.max(1L, Math.round(1.0 / multiplier));
        return bucket.tryConsume(tokens);
    }

    private void reject(HttpServletResponse response) throws IOException {
        metrics.incrementRateLimitRejects();
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        response.getWriter().write("{\"title\":\"Too Many Requests\",\"status\":429,\"detail\":\"Rate limit exceeded\"}");
    }

    private Bucket createBucket(RateLimitBucketProperties limit) {
        Bandwidth bandwidth = Bandwidth.builder()
                .capacity(limit.getCapacity())
                .refillGreedy(limit.getRefillPerSecond(), Duration.ofSeconds(1))
                .build();
        return Bucket.builder().addLimit(bandwidth).build();
    }

    private String resolveClientIp(HttpServletRequest request) {
        String forwarded = request.getHeader("X-Forwarded-For");
        if (forwarded != null && !forwarded.isBlank()) {
            return forwarded.split(",")[0].trim();
        }
        return request.getRemoteAddr();
    }

    private Long extractCustomerId(ContentCachingRequestWrapper request) {
        byte[] content = request.getContentAsByteArray();
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
