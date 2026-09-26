package dev.marcosjesus.orderevents.inventory;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumidor independente: reserva estoque quando um pedido é criado.
 * Fica em um consumer group próprio para não competir com outros consumidores do mesmo tópico.
 */
@Component
public class InventoryReservationListener {

    private static final Logger log = LoggerFactory.getLogger(InventoryReservationListener.class);

    @KafkaListener(id = "inventory-service", clientIdPrefix = "inventory-service", topics = KafkaTopics.ORDER_CREATED, groupId = "inventory-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.debug("[inventory-service] Reservando {} unidade(s) de '{}' para o pedido {}",
                event.quantity(), event.product(), event.orderId());
    }
}
