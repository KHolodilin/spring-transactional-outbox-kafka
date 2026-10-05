package com.kholodilin.outbox.config;

import liquibase.integration.spring.SpringLiquibase;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * The reactive idempotency starter creates {@code idempotencySchemaManager} with
 * {@code initMethod=initializeBlocking} and {@code schema.mode=validate}. R2DBC beans
 * do not wait for JDBC Liquibase, so a fresh Docker volume fails before
 * {@code 006-idempotency-records.sql} runs.
 */
@Configuration
public class IdempotencySchemaLiquibaseOrderConfig {

    static final String SCHEMA_MANAGER_BEAN = "idempotencySchemaManager";

    @Bean
    static BeanFactoryPostProcessor idempotencySchemaManagerAfterLiquibase() {
        return IdempotencySchemaLiquibaseOrderConfig::delaySchemaManagerUntilLiquibase;
    }

    static void delaySchemaManagerUntilLiquibase(ConfigurableListableBeanFactory beanFactory) {
        if (!beanFactory.containsBeanDefinition(SCHEMA_MANAGER_BEAN)) {
            return;
        }

        String[] liquibaseBeans = beanFactory.getBeanNamesForType(SpringLiquibase.class, true, false);
        if (liquibaseBeans.length == 0) {
            return;
        }

        BeanDefinition definition = beanFactory.getBeanDefinition(SCHEMA_MANAGER_BEAN);
        definition.setDependsOn(mergeDependsOn(definition.getDependsOn(), liquibaseBeans));
    }

    static String[] mergeDependsOn(String[] existing, String[] extra) {
        Set<String> merged = new LinkedHashSet<>();
        if (existing != null) {
            merged.addAll(Arrays.asList(existing));
        }
        merged.addAll(Arrays.asList(extra));
        return merged.toArray(String[]::new);
    }
}
