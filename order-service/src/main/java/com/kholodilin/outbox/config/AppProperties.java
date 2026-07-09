package com.kholodilin.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/** Typed configuration bound from {@code app.*} keys in application.yml. */
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    /** Unique pod/instance id used as {@code locked_by} for outbox leases. */
    private String instanceId = "local";
    private KafkaProperties kafka = new KafkaProperties();
    private OutboxProperties outbox = new OutboxProperties();
    private RateLimitProperties rateLimit = new RateLimitProperties();
    private HealthProperties health = new HealthProperties();

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public KafkaProperties getKafka() {
        return kafka;
    }

    public void setKafka(KafkaProperties kafka) {
        this.kafka = kafka;
    }

    public OutboxProperties getOutbox() {
        return outbox;
    }

    public void setOutbox(OutboxProperties outbox) {
        this.outbox = outbox;
    }

    public RateLimitProperties getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(RateLimitProperties rateLimit) {
        this.rateLimit = rateLimit;
    }

    public HealthProperties getHealth() {
        return health;
    }

    public void setHealth(HealthProperties health) {
        this.health = health;
    }
}
