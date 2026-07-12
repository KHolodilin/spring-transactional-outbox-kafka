package com.kholodilin.outbox;

import com.kholodilin.outbox.support.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@ActiveProfiles("test")
class OrderServiceApplicationIT extends AbstractIntegrationTest {

    @Test
    void contextLoads() {
    }
}
