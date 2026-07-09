package com.kholodilin.outbox.config;

/** In-memory queue, publisher worker, and recovery worker tuning. */
public class OutboxProperties {

    private MemoryQueueProperties memoryQueue = new MemoryQueueProperties();
    private PublisherProperties publisher = new PublisherProperties();
    private RecoveryProperties recovery = new RecoveryProperties();

    public MemoryQueueProperties getMemoryQueue() {
        return memoryQueue;
    }

    public void setMemoryQueue(MemoryQueueProperties memoryQueue) {
        this.memoryQueue = memoryQueue;
    }

    public PublisherProperties getPublisher() {
        return publisher;
    }

    public void setPublisher(PublisherProperties publisher) {
        this.publisher = publisher;
    }

    public RecoveryProperties getRecovery() {
        return recovery;
    }

    public void setRecovery(RecoveryProperties recovery) {
        this.recovery = recovery;
    }
}
