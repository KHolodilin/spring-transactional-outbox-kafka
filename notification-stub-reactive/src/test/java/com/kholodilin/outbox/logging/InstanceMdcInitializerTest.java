package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.NotificationStubProperties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class InstanceMdcInitializerTest {

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void initSetsInstanceIdInMdc() {
        NotificationStubProperties properties = new NotificationStubProperties();
        properties.setInstanceId("stub-pod");
        InstanceMdcInitializer initializer = new InstanceMdcInitializer(properties);

        ReflectionTestUtils.invokeMethod(initializer, "init");

        assertThat(MDC.get("instance.id")).isEqualTo("stub-pod");
    }

    @Test
    void enrichAndClearConsumerContextRestoreInstanceFields() {
        NotificationStubProperties properties = new NotificationStubProperties();
        properties.setInstanceId("stub-pod");
        InstanceMdcInitializer initializer = new InstanceMdcInitializer(properties);
        MDC.put("correlationId", "temp");

        initializer.enrich();
        assertThat(MDC.get("instance.id")).isEqualTo("stub-pod");

        initializer.clearConsumerContext();
        assertThat(MDC.get("correlationId")).isNull();
        assertThat(MDC.get("instance.id")).isEqualTo("stub-pod");
    }
}
