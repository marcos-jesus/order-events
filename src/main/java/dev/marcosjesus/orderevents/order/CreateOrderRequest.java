package dev.marcosjesus.orderevents.order;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.math.BigDecimal;

public record CreateOrderRequest(
        @NotBlank String product,
        @Positive int quantity,
        @NotNull @Positive BigDecimal price
) {
}
