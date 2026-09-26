package dev.marcosjesus.orderevents.kafka;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

/**
 * Decora o recoverer da dead-letter topic contando cada mensagem que esgotou as tentativas.
 * Só conta depois que a publicação na DLT deu certo, para não contar duas vezes quando o
 * error handler repete a recuperação. O contador nasce com valor 0, assim a série existe
 * no endpoint Prometheus antes da primeira falha.
 */
public class CountingRecoverer implements ConsumerRecordRecoverer {

    private final ConsumerRecordRecoverer delegate;
    private final Counter deadLettered;

    public CountingRecoverer(ConsumerRecordRecoverer delegate, MeterRegistry registry) {
        this.delegate = delegate;
        this.deadLettered = Counter.builder("orders.dead_lettered")
                .description("Mensagens enviadas para a dead-letter topic após esgotar as tentativas")
                .register(registry);
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        delegate.accept(record, exception);
        deadLettered.increment();
    }
}
