package com.kholodilin.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Order API, transactional outbox, in-memory queue, and Kafka publisher. */
@SpringBootApplication
@EnableScheduling
public class OrderServiceApplication {

    /**
     * Boots the order-service Spring context (API, outbox, publisher, recovery).
     *
     * @param args standard Spring Boot arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
