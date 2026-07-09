package com.kholodilin.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

/** Typed configuration bound from {@code app.*} keys in application.yml. */
@ConfigurationProperties(prefix = "app")
public class AppProperties {

    /** Unique pod/instance id used as {@code locked_by} for outbox leases. */
    private String instanceId = "local";
    private Kafka kafka = new Kafka();
    private Outbox outbox = new Outbox();
    private RateLimit rateLimit = new RateLimit();
    private Health health = new Health();

    public String getInstanceId() {
        return instanceId;
    }

    public void setInstanceId(String instanceId) {
        this.instanceId = instanceId;
    }

    public Kafka getKafka() {
        return kafka;
    }

    public void setKafka(Kafka kafka) {
        this.kafka = kafka;
    }

    public Outbox getOutbox() {
        return outbox;
    }

    public void setOutbox(Outbox outbox) {
        this.outbox = outbox;
    }

    public RateLimit getRateLimit() {
        return rateLimit;
    }

    public void setRateLimit(RateLimit rateLimit) {
        this.rateLimit = rateLimit;
    }

    public Health getHealth() {
        return health;
    }

    public void setHealth(Health health) {
        this.health = health;
    }

    /** Kafka topic and publisher settings. */
    public static class Kafka {
        private String topic = "orders.events";

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }
    }

    /** In-memory queue, publisher worker, and recovery worker tuning. */
    public static class Outbox {
        private MemoryQueue memoryQueue = new MemoryQueue();
        private Publisher publisher = new Publisher();
        private Recovery recovery = new Recovery();

        public MemoryQueue getMemoryQueue() {
            return memoryQueue;
        }

        public void setMemoryQueue(MemoryQueue memoryQueue) {
            this.memoryQueue = memoryQueue;
        }

        public Publisher getPublisher() {
            return publisher;
        }

        public void setPublisher(Publisher publisher) {
            this.publisher = publisher;
        }

        public Recovery getRecovery() {
            return recovery;
        }

        public void setRecovery(Recovery recovery) {
            this.recovery = recovery;
        }

        /** Bounded per-pod queue between DB commit and Kafka publish. */
        public static class MemoryQueue {
            private int capacity = 10000;
            private int batchSize = 100;
            private Duration batchWait = Duration.ofMillis(50);
            private double usageThreshold = 0.8;

            public int getCapacity() {
                return capacity;
            }

            public void setCapacity(int capacity) {
                this.capacity = capacity;
            }

            public int getBatchSize() {
                return batchSize;
            }

            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }

            public Duration getBatchWait() {
                return batchWait;
            }

            public void setBatchWait(Duration batchWait) {
                this.batchWait = batchWait;
            }

            public double getUsageThreshold() {
                return usageThreshold;
            }

            public void setUsageThreshold(double usageThreshold) {
                this.usageThreshold = usageThreshold;
            }
        }

        /** Batch publisher lease, retry, and poll interval. */
        public static class Publisher {
            private Duration leaseDuration = Duration.ofSeconds(30);
            private int maxRetries = 5;
            private Duration pollInterval = Duration.ofMillis(100);

            public Duration getLeaseDuration() {
                return leaseDuration;
            }

            public void setLeaseDuration(Duration leaseDuration) {
                this.leaseDuration = leaseDuration;
            }

            public int getMaxRetries() {
                return maxRetries;
            }

            public void setMaxRetries(int maxRetries) {
                this.maxRetries = maxRetries;
            }

            public Duration getPollInterval() {
                return pollInterval;
            }

            public void setPollInterval(Duration pollInterval) {
                this.pollInterval = pollInterval;
            }
        }

        /** Scheduled scan of stuck ACTIVE rows — enqueue only, never publishes directly. */
        public static class Recovery {
            private boolean enabled = true;
            private Duration interval = Duration.ofSeconds(10);
            private int batchSize = 500;

            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public Duration getInterval() {
                return interval;
            }

            public void setInterval(Duration interval) {
                this.interval = interval;
            }

            public int getBatchSize() {
                return batchSize;
            }

            public void setBatchSize(int batchSize) {
                this.batchSize = batchSize;
            }
        }
    }

    /** Token-bucket limits with adaptive backpressure when queue pressure is high. */
    public static class RateLimit {
        private double throttleMultiplier = 0.5;
        private Limit global = new Limit(1000, 100);
        private Limit perCustomer = new Limit(100, 10);
        private Limit perIp = new Limit(50, 5);

        public double getThrottleMultiplier() {
            return throttleMultiplier;
        }

        public void setThrottleMultiplier(double throttleMultiplier) {
            this.throttleMultiplier = throttleMultiplier;
        }

        public Limit getGlobal() {
            return global;
        }

        public void setGlobal(Limit global) {
            this.global = global;
        }

        public Limit getPerCustomer() {
            return perCustomer;
        }

        public void setPerCustomer(Limit perCustomer) {
            this.perCustomer = perCustomer;
        }

        public Limit getPerIp() {
            return perIp;
        }

        public void setPerIp(Limit perIp) {
            this.perIp = perIp;
        }

        public static class Limit {
            private long capacity;
            private long refillPerSecond;

            public Limit() {
            }

            public Limit(long capacity, long refillPerSecond) {
                this.capacity = capacity;
                this.refillPerSecond = refillPerSecond;
            }

            public long getCapacity() {
                return capacity;
            }

            public void setCapacity(long capacity) {
                this.capacity = capacity;
            }

            public long getRefillPerSecond() {
                return refillPerSecond;
            }

            public void setRefillPerSecond(long refillPerSecond) {
                this.refillPerSecond = refillPerSecond;
            }
        }
    }

    /** Thresholds for Actuator health indicators (queue pressure, pending outbox rows). */
    public static class Health {
        private double queuePressureCritical = 0.95;
        private long outboxPendingCritical = 10000;

        public double getQueuePressureCritical() {
            return queuePressureCritical;
        }

        public void setQueuePressureCritical(double queuePressureCritical) {
            this.queuePressureCritical = queuePressureCritical;
        }

        public long getOutboxPendingCritical() {
            return outboxPendingCritical;
        }

        public void setOutboxPendingCritical(long outboxPendingCritical) {
            this.outboxPendingCritical = outboxPendingCritical;
        }
    }
}
