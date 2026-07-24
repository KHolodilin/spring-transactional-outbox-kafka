package com.kholodilin.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/** Reactive order API, R2DBC outbox, in-memory queue, and Kafka publisher. */
@SpringBootApplication
public class OrderServiceReactiveApplication {

    public static void main(String[] args) {
        SpringApplication.run(OrderServiceReactiveApplication.class, args);
    }
}
