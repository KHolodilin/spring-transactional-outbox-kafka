package com.kholodilin.outbox.tracing;

import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;
import io.micrometer.observation.contextpropagation.ObservationThreadLocalAccessor;
import io.micrometer.observation.transport.ReceiverContext;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;

@Component
@RequiredArgsConstructor
public class TraceContextSupport {

    private final ObservationRegistry observationRegistry;

    public <T> Mono<T> withRecordTrace(
            ConsumerRecord<?, ?> record,
            String observationName,
            Supplier<Mono<T>> action
    ) {
        return Mono.defer(() -> {

            KafkaReceiverContext receiverContext =
                    new KafkaReceiverContext(record);

            Observation observation =
                    Observation.createNotStarted(
                                    observationName,
                                    () -> receiverContext,
                                    observationRegistry
                            )
                            .contextualName(observationName)
                            .start();

            return action.get()
                    .doOnError(observation::error)
                    .doFinally(signalType ->
                                       observation.stop()
                    )
                    .contextWrite(context ->
                                          context.put(
                                                  ObservationThreadLocalAccessor.KEY,
                                                  observation
                                          )
                    );
        });
    }

    private static final class KafkaReceiverContext
            extends ReceiverContext<ConsumerRecord<?, ?>> {

        private KafkaReceiverContext(
                ConsumerRecord<?, ?> record
        ) {
            super((carrier, key) -> {

                Header header =
                        carrier.headers().lastHeader(key);

                if (header == null || header.value() == null) {
                    return null;
                }

                return new String(
                        header.value(),
                        StandardCharsets.UTF_8
                );
            });

            setCarrier(record);
        }
    }
}