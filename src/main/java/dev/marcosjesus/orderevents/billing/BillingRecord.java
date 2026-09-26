package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Registro de faturamento persistido no Postgres para cada {@link OrderCreatedEvent}
 * consumido pelo {@link BillingObservableListener}. A chave é o próprio orderId: como o
 * Kafka entrega no mínimo uma vez, reprocessar o mesmo evento apenas regrava o mesmo
 * registro (idempotente via upsert do Spring Data), sem duplicar faturamento.
 */
@Entity
@Table(name = "billing_record")
public class BillingRecord {

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(nullable = false)
    private String product;

    @Column(nullable = false)
    private int quantity;

    @Column(name = "unit_price", nullable = false)
    private BigDecimal unitPrice;

    @Column(name = "total_amount", nullable = false)
    private BigDecimal totalAmount;

    @Column(name = "order_created_at", nullable = false)
    private Instant orderCreatedAt;

    protected BillingRecord() {
        // exigido pelo JPA
    }

    private BillingRecord(UUID orderId, String product, int quantity, BigDecimal unitPrice,
                           BigDecimal totalAmount, Instant orderCreatedAt) {
        this.orderId = orderId;
        this.product = product;
        this.quantity = quantity;
        this.unitPrice = unitPrice;
        this.totalAmount = totalAmount;
        this.orderCreatedAt = orderCreatedAt;
    }

    public static BillingRecord from(OrderCreatedEvent event) {
        BigDecimal total = event.price().multiply(BigDecimal.valueOf(event.quantity()));
        return new BillingRecord(event.orderId(), event.product(), event.quantity(), event.price(),
                total, event.createdAt());
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getProduct() {
        return product;
    }

    public int getQuantity() {
        return quantity;
    }

    public BigDecimal getUnitPrice() {
        return unitPrice;
    }

    public BigDecimal getTotalAmount() {
        return totalAmount;
    }

    public Instant getOrderCreatedAt() {
        return orderCreatedAt;
    }
}
