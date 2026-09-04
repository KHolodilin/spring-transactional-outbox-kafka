package com.kholodilin.outbox.notification;

import com.kholodilin.idempotency.ExecutionResult;
import com.kholodilin.idempotency.IdempotencyCall;
import com.kholodilin.idempotency.IdempotencyService;
import com.kholodilin.outbox.events.EventEnvelope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationTransactionServiceTest {

    @Mock
    private IdempotencyService idempotencyService;

    @Mock
    private IdempotencyCall idempotencyCall;

    private NotificationTransactionService service;

    @BeforeEach
    void setUp() {
        service = new NotificationTransactionService(idempotencyService);
    }

    @Test
    void firstEventExecutesNotificationAction() {
        EventEnvelope event = event(42L);
        stubCall(event);
        when(idempotencyCall.execute(eq(Void.class), any())).thenAnswer(invocation -> {
            Supplier<ExecutionResult<Void>> action = invocation.getArgument(1);
            return action.get();
        });

        boolean sent = service.process(event);

        assertThat(sent).isTrue();
        verify(idempotencyService).operation("NOTIFY_ORDER_CREATED");
        verify(idempotencyCall).key("42");
        verify(idempotencyCall).request(event);
    }

    @Test
    void replaySkipsNotificationAction() {
        EventEnvelope event = event(42L);
        stubCall(event);
        when(idempotencyCall.execute(eq(Void.class), any())).thenReturn(ExecutionResult.success(null));

        boolean sent = service.process(event);

        assertThat(sent).isFalse();
    }

    private void stubCall(EventEnvelope event) {
        when(idempotencyService.operation("NOTIFY_ORDER_CREATED")).thenReturn(idempotencyCall);
        when(idempotencyCall.key("42")).thenReturn(idempotencyCall);
        when(idempotencyCall.request(event)).thenReturn(idempotencyCall);
    }

    private EventEnvelope event(long eventId) {
        return new EventEnvelope(
                eventId,
                2L,
                3L,
                "OrderCreated",
                Map.of("orderId", 2),
                "corr",
                null,
                null
        );
    }
}
