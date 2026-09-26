package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BillingObservableListener {

    private static final Logger log = LoggerFactory.getLogger(BillingObservableListener.class);

    private final BillingRecordRepository repository;

    public BillingObservableListener(BillingRecordRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(id = "billing-service", topics = KafkaTopics.ORDER_CREATED, groupId = "billing-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        repository.save(BillingRecord.from(event));
        log.info("[billing-service] faturamento salvo no banco para o pedido {}.", event.orderId());
    }
}
