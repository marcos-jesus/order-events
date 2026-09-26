package dev.marcosjesus.orderevents.billing;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Grava faturamentos em lote com um único upsert (INSERT ... ON CONFLICT DO UPDATE), em vez de
 * um SELECT + INSERT por linha como o save() do JPA faz com id atribuído. Tudo numa transação:
 * ou o lote inteiro é gravado, ou nada. O upsert mantém a idempotência da entrega at-least-once.
 */
@Component
public class BillingBatchWriter {

    private static final String UPSERT = """
            INSERT INTO billing_record
                (order_id, product, quantity, unit_price, total_amount, order_created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT (order_id) DO UPDATE SET
                product = EXCLUDED.product,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                total_amount = EXCLUDED.total_amount,
                order_created_at = EXCLUDED.order_created_at
            """;

    private final JdbcTemplate jdbc;
    private final int batchSize;

    public BillingBatchWriter(JdbcTemplate jdbc, BillingProperties properties) {
        this.jdbc = jdbc;
        this.batchSize = properties.batchSize();
    }

    /**
     * @return quantas linhas distintas foram gravadas (duplicatas por orderId na mesma chamada
     *         contam uma vez: o INSERT multi-linha do Postgres não aceita a mesma chave duas vezes)
     */
    @Transactional
    public int write(Collection<BillingRecord> records) {
        if (records.isEmpty()) {
            return 0;
        }
        Map<UUID, BillingRecord> distinct = new LinkedHashMap<>();
        for (BillingRecord record : records) {
            distinct.put(record.getOrderId(), record);
        }
        List<BillingRecord> rows = List.copyOf(distinct.values());
        jdbc.batchUpdate(UPSERT, rows, batchSize, (ps, record) -> {
            ps.setObject(1, record.getOrderId());
            ps.setString(2, record.getProduct());
            ps.setInt(3, record.getQuantity());
            ps.setBigDecimal(4, record.getUnitPrice());
            ps.setBigDecimal(5, record.getTotalAmount());
            ps.setTimestamp(6, Timestamp.from(record.getOrderCreatedAt()));
        });
        return rows.size();
    }
}
