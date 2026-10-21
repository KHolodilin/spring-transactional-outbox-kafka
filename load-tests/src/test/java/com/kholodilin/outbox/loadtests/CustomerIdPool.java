package com.kholodilin.outbox.loadtests;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-robin customer ids {@code 1..size}. Kafka uses {@code customerId} as the partition key,
 * so a single id pins every event on one partition.
 */
final class CustomerIdPool {

    private final long size;
    private final AtomicLong sequence = new AtomicLong();

    CustomerIdPool(long size) {
        if (size < 1) {
            throw new IllegalArgumentException("customerPoolSize must be at least 1");
        }
        this.size = size;
    }

    long next() {
        return Math.floorMod(sequence.getAndIncrement(), size) + 1;
    }
}
