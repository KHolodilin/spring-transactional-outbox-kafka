package com.kholodilin.outbox.config;

import liquibase.integration.spring.SpringLiquibase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencySchemaLiquibaseOrderConfigTest {

    @Test
    void schemaManagerDependsOnLiquibaseBean() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition("liquibase", new RootBeanDefinition(SpringLiquibase.class));
        beanFactory.registerBeanDefinition(
                IdempotencySchemaLiquibaseOrderConfig.SCHEMA_MANAGER_BEAN,
                new RootBeanDefinition(Object.class)
        );

        IdempotencySchemaLiquibaseOrderConfig.delaySchemaManagerUntilLiquibase(beanFactory);

        assertThat(beanFactory.getBeanDefinition(IdempotencySchemaLiquibaseOrderConfig.SCHEMA_MANAGER_BEAN).getDependsOn())
                .contains("liquibase");
    }

    @Test
    void skipsWhenSchemaManagerIsAbsent() {
        DefaultListableBeanFactory beanFactory = new DefaultListableBeanFactory();
        beanFactory.registerBeanDefinition("liquibase", new RootBeanDefinition(SpringLiquibase.class));

        IdempotencySchemaLiquibaseOrderConfig.delaySchemaManagerUntilLiquibase(beanFactory);

        assertThat(beanFactory.containsBeanDefinition(IdempotencySchemaLiquibaseOrderConfig.SCHEMA_MANAGER_BEAN))
                .isFalse();
    }

    @Test
    void mergeDependsOnKeepsExistingAndAddsLiquibase() {
        assertThat(IdempotencySchemaLiquibaseOrderConfig.mergeDependsOn(
                new String[] {"other"},
                new String[] {"liquibase"}
        )).containsExactly("other", "liquibase");
    }
}
