package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.AppProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.core.Ordered;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/** Adds instance and tracing alias fields to MDC for every HTTP request. */
@Component
@RequiredArgsConstructor
public class LogMdcEnricherWebFilter implements WebFilter, Ordered {

    private final AppProperties appProperties;

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        StructuredLogContext.putInstanceFields(appProperties.getInstanceId());
        StructuredLogContext.enrichTracingAliases();
        return chain.filter(exchange)
                .doFinally(signal -> StructuredLogContext.enrichTracingAliases());
    }
}
