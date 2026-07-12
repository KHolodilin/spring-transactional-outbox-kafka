package com.kholodilin.outbox;

import com.kholodilin.outbox.support.AbstractIntegrationTest;
import com.kholodilin.outbox.support.KafkaPublisherMockTestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
@Import(KafkaPublisherMockTestConfig.class)
class OrderServiceApplicationIT extends AbstractIntegrationTest {

    @Test
    void contextLoads() {
    }
}
