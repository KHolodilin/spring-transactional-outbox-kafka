package com.kholodilin.outbox.logging;

import org.springframework.core.Ordered;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/** Clears request-scoped MDC after order API handling to avoid leaks across event-loop threads. */
@Component
public class HttpRequestMdcWebFilter implements WebFilter, Ordered {

    @Override
    public int getOrder() {
        return Ordered.LOWEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        if (!HttpMethod.POST.equals(exchange.getRequest().getMethod())
                || !exchange.getRequest().getPath().value().endsWith("/api/v1/orders")) {
            return chain.filter(exchange);
        }
        return chain.filter(exchange)
                .doFinally(signal -> StructuredLogContext.clearRequestContext());
    }
}
