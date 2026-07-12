package com.kholodilin.outbox.api;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.SerializationFeature;
import com.kholodilin.outbox.events.CreateOrderRequest;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Produces a stable SHA-256 hash of the request body for idempotency comparison.
 * <p>
 * JSON keys are sorted so equivalent payloads hash identically regardless of field order.
 */
@Component
@RequiredArgsConstructor
public class RequestHashCalculator {

    private final ObjectMapper objectMapper;

    private ObjectMapper canonicalMapper;

    @PostConstruct
    void init() {
        canonicalMapper = objectMapper.rebuild()
                .configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true)
                .build();
    }

    public String calculate(CreateOrderRequest request) {
        try {
            String canonicalJson = canonicalMapper.writeValueAsString(request);
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonicalJson.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to calculate request hash", ex);
        }
    }
}
