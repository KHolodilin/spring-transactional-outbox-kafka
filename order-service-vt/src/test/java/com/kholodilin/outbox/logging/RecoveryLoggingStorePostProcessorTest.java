package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecoveryLoggingStorePostProcessorTest {

    @Test
    void wrapsOutboxStoreOnce() {
        OutboxStore store = mock(OutboxStore.class);
        ObjectProvider<AppProperties> properties = mock(ObjectProvider.class);
        when(properties.getIfAvailable()).thenReturn(AppProperties.builder().instanceId("pod-1").build());
        RecoveryLoggingStorePostProcessor processor = new RecoveryLoggingStorePostProcessor(properties);

        Object wrapped = processor.postProcessAfterInitialization(store, "outboxStore");

        assertThat(wrapped).isInstanceOf(RecoveryLoggingOutboxStore.class);
        assertThat(processor.postProcessAfterInitialization(wrapped, "outboxStore")).isSameAs(wrapped);
    }

    @Test
    void leavesNonStoreBeansUnchanged() {
        Object bean = new Object();
        ObjectProvider<AppProperties> properties = mock(ObjectProvider.class);
        RecoveryLoggingStorePostProcessor processor = new RecoveryLoggingStorePostProcessor(properties);

        assertThat(processor.postProcessAfterInitialization(bean, "other")).isSameAs(bean);
    }

    @Test
    void usesLocalInstanceIdWhenPropertiesMissing() {
        OutboxStore store = mock(OutboxStore.class);
        ObjectProvider<AppProperties> properties = mock(ObjectProvider.class);
        when(properties.getIfAvailable()).thenReturn(null);
        RecoveryLoggingStorePostProcessor processor = new RecoveryLoggingStorePostProcessor(properties);

        Object wrapped = processor.postProcessAfterInitialization(store, "outboxStore");

        assertThat(wrapped).isInstanceOf(RecoveryLoggingOutboxStore.class);
    }
}
