package dev.marcosjesus.orderevents.order;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        String product,
        int quantity,
        BigDecimal price,
        Instant createdAt
) {

    public static OrderCreatedEvent from(CreateOrderRequest request) {
        return new OrderCreatedEvent(UUID.randomUUID(), request.product(), request.quantity(), request.price(), Instant.now());
    }
}
