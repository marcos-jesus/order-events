package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

/**
 * Consome os eventos em lote e grava cada poll numa transação (ver {@link BillingBatchWriter}).
 * O timer spring.kafka.listener conta por lote, não por item, então a vazão real de itens
 * gravados vai em orders.billing.persisted e o tamanho de cada lote em orders.billing.batch.size.
 */
@Component
public class BillingObservableListener {

    private static final Logger log = LoggerFactory.getLogger(BillingObservableListener.class);

    private final BillingBatchWriter writer;
    private final Counter persisted;
    private final DistributionSummary batchSize;

    public BillingObservableListener(BillingBatchWriter writer, MeterRegistry registry) {
        this.writer = writer;
        this.persisted = Counter.builder("orders.billing.persisted")
                .description("Faturamentos gravados no banco")
                .register(registry);
        this.batchSize = DistributionSummary.builder("orders.billing.batch.size")
                .description("Eventos por lote gravado no banco")
                .register(registry);
    }

    @KafkaListener(id = "billing-service", clientIdPrefix = "billing-service", topics = KafkaTopics.ORDER_CREATED, groupId = "billing-service",
            containerFactory = "billingBatchContainerFactory")
    public void onOrderCreated(List<OrderCreatedEvent> events) {
        // payloads que o ErrorHandlingDeserializer não conseguiu ler chegam como null
        var records = events.stream().filter(Objects::nonNull).map(BillingRecord::from).toList();
        int gravados = writer.write(records);
        persisted.increment(gravados);
        batchSize.record(records.size());
        log.info("[billing-service] {} faturamento(s) salvo(s) no banco em um lote.", gravados);
    }
}
