package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.config.AppProperties;
import com.kholodilin.outbox.spi.OutboxStore;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.stereotype.Component;

/** Wraps the starter {@link OutboxStore} so recovery ticks emit the shared structured log action. */
@Component
@RequiredArgsConstructor
public class RecoveryLoggingStorePostProcessor implements BeanPostProcessor {

    private final ObjectProvider<AppProperties> appProperties;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (bean instanceof OutboxStore store && !(bean instanceof RecoveryLoggingOutboxStore)) {
            AppProperties properties = appProperties.getIfAvailable();
            String instanceId = properties == null ? "local" : properties.getInstanceId();
            return new RecoveryLoggingOutboxStore(store, instanceId);
        }
        return bean;
    }
}
