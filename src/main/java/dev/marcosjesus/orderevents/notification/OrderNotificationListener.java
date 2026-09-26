package dev.marcosjesus.orderevents.notification;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Segundo consumidor do mesmo tópico, em um consumer group separado, para demonstrar
 * o fan-out do Kafka: múltiplos serviços reagem ao mesmo evento de forma desacoplada.
 */
@Component
public class OrderNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(OrderNotificationListener.class);

    @KafkaListener(id = "notification-service", clientIdPrefix = "notification-service", topics = KafkaTopics.ORDER_CREATED, groupId = "notification-service")
    public void onOrderCreated(OrderCreatedEvent event) {
        log.debug("[notification-service] Enviando confirmação do pedido {} ({}x {})",
                event.orderId(), event.quantity(), event.product());
    }
}
