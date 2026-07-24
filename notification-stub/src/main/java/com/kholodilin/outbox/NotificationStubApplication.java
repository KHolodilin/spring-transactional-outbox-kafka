package com.kholodilin.outbox;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;

/** Demo downstream service — consumes order events and logs mock notifications. */
@SpringBootApplication
@EnableKafka
public class NotificationStubApplication {

    /**
     * Boots the notification-stub Spring context (Kafka consumer demo).
     *
     * @param args standard Spring Boot arguments
     */
    public static void main(String[] args) {
        SpringApplication.run(NotificationStubApplication.class, args);
    }
}
