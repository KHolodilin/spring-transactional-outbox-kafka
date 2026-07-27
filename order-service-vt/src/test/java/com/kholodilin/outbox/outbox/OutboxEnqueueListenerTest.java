package com.kholodilin.outbox.outbox;

import com.kholodilin.outbox.queue.InMemoryEventQueue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxEnqueueListenerTest {

    @Mock
    private InMemoryEventQueue eventQueue;

    @AfterEach
    void tearDown() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clear();
        }
    }

    @Test
    void enqueuesImmediatelyWhenNoTransaction() {
        OutboxEnqueueListener listener = new OutboxEnqueueListener(eventQueue);
        when(eventQueue.enqueue(42L)).thenReturn(true);

        listener.enqueueAfterCommit(42L);

        verify(eventQueue).enqueue(42L);
    }

    @Test
    void enqueuesAfterCommitWhenTransactionActive() {
        OutboxEnqueueListener listener = new OutboxEnqueueListener(eventQueue);
        TransactionSynchronizationManager.initSynchronization();
        try {
            listener.enqueueAfterCommit(42L);
            verify(eventQueue, never()).enqueue(42L);

            when(eventQueue.enqueue(42L)).thenReturn(true);
            for (TransactionSynchronization synchronization : TransactionSynchronizationManager.getSynchronizations()) {
                synchronization.afterCommit();
            }
            verify(eventQueue).enqueue(42L);
        } finally {
            TransactionSynchronizationManager.clear();
        }
    }
}
