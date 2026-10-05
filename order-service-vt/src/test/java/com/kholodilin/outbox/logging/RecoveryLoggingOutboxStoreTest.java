package com.kholodilin.outbox.logging;

import com.kholodilin.outbox.events.ObservabilityVocabulary;
import com.kholodilin.outbox.model.OutboxInsert;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.model.OutboxStatus;
import com.kholodilin.outbox.spi.OutboxStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.MDC;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecoveryLoggingOutboxStoreTest {

    @Mock
    private OutboxStore delegate;

    private RecoveryLoggingOutboxStore store;

    @BeforeEach
    void setUp() {
        store = new RecoveryLoggingOutboxStore(delegate, "pod-1");
    }

    @AfterEach
    void tearDown() {
        MDC.clear();
    }

    @Test
    void clearLeaseLogsRecoveryCompleted() {
        store.clearLease(List.of(1L, 2L));

        verify(delegate).clearLease(List.of(1L, 2L));
        assertThat(MDC.get("event.action")).isEqualTo(ObservabilityVocabulary.OUTBOX_RECOVERY_COMPLETED);
        assertThat(MDC.get("outbox.batch_size")).isEqualTo("2");
        assertThat(MDC.get("instance.id")).isEqualTo("pod-1");
    }

    @Test
    void clearLeaseSkipsLogWhenEmpty() {
        store.clearLease(List.of());

        verify(delegate).clearLease(List.of());
        assertThat(MDC.get("event.action")).isNull();
    }

    @Test
    void clearLeaseSkipsLogWhenNull() {
        store.clearLease(null);

        verify(delegate).clearLease(null);
        assertThat(MDC.get("event.action")).isNull();
    }

    @Test
    void delegatesRemainingStoreMethods() {
        OutboxInsert insert = mock(OutboxInsert.class);
        OutboxRecord record = mock(OutboxRecord.class);
        Instant sentAt = Instant.parse("2026-08-13T12:00:00Z");
        when(delegate.insert(insert)).thenReturn(11L);
        when(delegate.claimByIds(List.of(11L), "pod-1", sentAt)).thenReturn(List.of(record));
        when(delegate.claimRecoverableIds(5, "pod-1", sentAt)).thenReturn(List.of(11L));
        when(delegate.findReenqueueableIds(List.of(11L))).thenReturn(List.of(11L));
        when(delegate.findByIds(List.of(11L))).thenReturn(List.of(record));

        assertThat(store.insert(insert)).isEqualTo(11L);
        assertThat(store.claimByIds(List.of(11L), "pod-1", sentAt)).containsExactly(record);
        store.markSent(List.of(11L), sentAt);
        store.markFailed(11L, 2, OutboxStatus.FAILED);
        assertThat(store.claimRecoverableIds(5, "pod-1", sentAt)).containsExactly(11L);
        assertThat(store.findReenqueueableIds(List.of(11L))).containsExactly(11L);
        assertThat(store.findByIds(List.of(11L))).containsExactly(record);

        verify(delegate).insert(insert);
        verify(delegate).claimByIds(List.of(11L), "pod-1", sentAt);
        verify(delegate).markSent(List.of(11L), sentAt);
        verify(delegate).markFailed(11L, 2, OutboxStatus.FAILED);
        verify(delegate).claimRecoverableIds(5, "pod-1", sentAt);
        verify(delegate).findReenqueueableIds(List.of(11L));
        verify(delegate).findByIds(List.of(11L));
    }
}
