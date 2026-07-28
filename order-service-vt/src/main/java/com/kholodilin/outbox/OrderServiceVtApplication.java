package com.kholodilin.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Order API on Virtual Threads: transactional outbox, in-memory queue, and Kafka publisher. */
@SpringBootApplication
@EnableScheduling
public class OrderServiceVtApplication {

    /**
     * Boots the order-service-vt Spring context (API, outbox, publisher, recovery).
     *
     * @param args standard Spring Boot arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(OrderServiceVtApplication.class, args);
    }
}
