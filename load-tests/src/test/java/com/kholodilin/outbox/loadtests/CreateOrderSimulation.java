package com.kholodilin.outbox.loadtests;

import io.gatling.javaapi.core.ScenarioBuilder;
import io.gatling.javaapi.core.Simulation;
import io.gatling.javaapi.http.HttpProtocolBuilder;

import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static io.gatling.javaapi.core.CoreDsl.*;
import static io.gatling.javaapi.http.HttpDsl.*;

/**
 * Baseline load test for {@code POST /api/v1/orders}.
 * <p>
 * Configure with system properties:
 * <ul>
 *   <li>{@code baseUrl} (default {@code http://localhost:8080})</li>
 *   <li>{@code rampSeconds} (default {@code 60})</li>
 *   <li>{@code stageDurationSeconds} (default {@code 300})</li>
 *   <li>{@code rps1}/{@code rps2}/{@code rps3} (defaults {@code 50/100/200})</li>
 * </ul>
 */
public class CreateOrderSimulation extends Simulation {

    private static final String BASE_URL = System.getProperty("baseUrl", "http://localhost:8080");

    private static final int RAMP_SECONDS = intProp("rampSeconds", 60);
    private static final int STAGE_DURATION_SECONDS = intProp("stageDurationSeconds", 300);

    private static final double RPS_1 = doubleProp("rps1", 50);
    private static final double RPS_2 = doubleProp("rps2", 100);
    private static final double RPS_3 = doubleProp("rps3", 200);

    private static final double MAX_FAILED_PERCENT = doubleProp("maxFailedPercent", 0.5);
    private static final int P95_MS = intProp("p95Ms", 200);

    private final HttpProtocolBuilder httpProtocol = http.baseUrl(BASE_URL)
            .contentTypeHeader("application/json")
            .acceptHeader("application/json");

    private final Iterator<Map<String, Object>> feeder = Stream.generate(() -> {
                long customerId = longProp("customerId", 42L);
                String correlationId = UUID.randomUUID().toString();
                String idempotencyKey = UUID.randomUUID().toString();
                return Map.<String, Object>of(
                        "customerId", customerId,
                        "correlationId", correlationId,
                        "idempotencyKey", idempotencyKey
                );
            })
            .iterator();

    private final ScenarioBuilder scn = scenario("CreateOrder")
            .feed(feeder)
            .exec(
                    http("POST /api/v1/orders")
                            .post("/api/v1/orders")
                            .header("Idempotency-Key", "#{idempotencyKey}")
                            .body(StringBody("""
                                    {
                                      "customerId": #{customerId},
                                      "items": [{"productId":"sku-1","quantity":2,"price":10.50}],
                                      "correlationId": "#{correlationId}"
                                    }
                                    """)).asJson()
                            .check(status().in(200, 201))
            );

    public CreateOrderSimulation() {
        Duration ramp = Duration.ofSeconds(RAMP_SECONDS);
        Duration stage = Duration.ofSeconds(STAGE_DURATION_SECONDS);

        setUp(
                scn.injectOpen(
                        rampUsersPerSec(0).to(RPS_1).during(ramp),
                        constantUsersPerSec(RPS_1).during(stage),
                        rampUsersPerSec(RPS_1).to(RPS_2).during(ramp),
                        constantUsersPerSec(RPS_2).during(stage),
                        rampUsersPerSec(RPS_2).to(RPS_3).during(ramp),
                        constantUsersPerSec(RPS_3).during(stage)
                )
        ).protocols(httpProtocol)
                .assertions(
                        global().failedRequests().percent().lt(MAX_FAILED_PERCENT),
                        global().responseTime().percentile3().lt(P95_MS)
                );
    }

    private static int intProp(String name, int defaultValue) {
        String value = System.getProperty(name);
        return value == null ? defaultValue : Integer.parseInt(value);
    }

    private static long longProp(String name, long defaultValue) {
        String value = System.getProperty(name);
        return value == null ? defaultValue : Long.parseLong(value);
    }

    private static double doubleProp(String name, double defaultValue) {
        String value = System.getProperty(name);
        return value == null ? defaultValue : Double.parseDouble(value);
    }
}

