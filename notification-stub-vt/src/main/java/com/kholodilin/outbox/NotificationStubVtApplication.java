package com.kholodilin.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/** Demo downstream service — consumes order events and logs mock notifications. */
@SpringBootApplication
@EnableKafka
public class NotificationStubVtApplication {

    /**
     * Boots the notification-stub-vt Spring context (Kafka consumer demo).
     *
     * @param args standard Spring Boot arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(NotificationStubVtApplication.class, args);
    }
}
