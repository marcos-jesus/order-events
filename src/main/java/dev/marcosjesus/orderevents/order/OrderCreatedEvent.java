package dev.marcosjesus.orderevents.order;

import java.time.Instant;
import java.util.UUID;

public record OrderCreatedEvent(
        UUID orderId,
        String product,
        int quantity,
        Instant createdAt
) {

    public static OrderCreatedEvent from(CreateOrderRequest request) {
        return new OrderCreatedEvent(UUID.randomUUID(), request.product(), request.quantity(), Instant.now());
    }
}
