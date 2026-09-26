package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class BillingRecordTest {

    @Test
    void calculaOValorTotalComoQuantidadeVezesPrecoUnitario() {
        UUID orderId = UUID.randomUUID();
        Instant createdAt = Instant.now();
        var event = new OrderCreatedEvent(orderId, "teclado-mecanico", 3, new BigDecimal("150.00"), createdAt);

        BillingRecord record = BillingRecord.from(event);

        assertThat(record.getOrderId()).isEqualTo(orderId);
        assertThat(record.getProduct()).isEqualTo("teclado-mecanico");
        assertThat(record.getQuantity()).isEqualTo(3);
        assertThat(record.getUnitPrice()).isEqualByComparingTo("150.00");
        assertThat(record.getTotalAmount()).isEqualByComparingTo("450.00");
        assertThat(record.getOrderCreatedAt()).isEqualTo(createdAt);
    }
}
