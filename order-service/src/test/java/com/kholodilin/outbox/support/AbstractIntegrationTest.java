package com.kholodilin.outbox.support;

import com.kholodilin.outbox.publisher.KafkaBatchPublisher;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

/** Shared wiring for order-service integration tests. */
public abstract class AbstractIntegrationTest {

    @MockitoBean
    protected KafkaBatchPublisher kafkaBatchPublisher;
}
