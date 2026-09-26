package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import dev.marcosjesus.orderevents.support.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.JdbcTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Sem transação de teste: o writer precisa abrir e commitar a própria transação, que é
 * justamente o comportamento (um lote = uma transação) que estes testes verificam.
 */
@JdbcTest
@AutoConfigureTestDatabase(replace = AutoConfigureTestDatabase.Replace.NONE)
@Import(BillingBatchWriter.class)
@EnableConfigurationProperties(BillingProperties.class)
@TestPropertySource(properties = "order-events.billing.batch-size=5000")
@Transactional(propagation = Propagation.NOT_SUPPORTED)
class BillingBatchWriterTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private BillingBatchWriter writer;

    @Autowired
    private JdbcTemplate jdbc;

    @BeforeEach
    void limpaTabela() {
        jdbc.execute("TRUNCATE billing_record");
    }

    @Test
    void gravaDozeMilRegistrosEmLotesDeCincoMil() {
        var registros = registros(12_000);

        int gravados = writer.write(registros);

        assertThat(gravados).isEqualTo(12_000);
        assertThat(contar()).isEqualTo(12_000);
    }

    @Test
    void reprocessarOMesmoEventoNaoDuplicaEAtualizaOFaturamento() {
        var id = UUID.randomUUID();
        writer.write(List.of(registro(id, "10.00", 2)));

        writer.write(List.of(registro(id, "20.00", 3)));

        assertThat(contar()).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT total_amount FROM billing_record WHERE order_id = ?",
                BigDecimal.class, id)).isEqualByComparingTo("60.00");
    }

    @Test
    void duplicataDentroDoMesmoLoteNaoQuebraOUpsert() {
        var id = UUID.randomUUID();

        int gravados = writer.write(List.of(registro(id, "10.00", 1), registro(id, "15.00", 1)));

        assertThat(gravados).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT unit_price FROM billing_record WHERE order_id = ?",
                BigDecimal.class, id)).isEqualByComparingTo("15.00");
    }

    @Test
    void listaVaziaNaoFazNada() {
        assertThat(writer.write(List.of())).isZero();
        assertThat(contar()).isZero();
    }

    @Test
    void falhaEmUmRegistroDesfazOLoteInteiroInclusiveOsLotesAnterioresDaMesmaChamada() {
        var registros = registros(6_000);
        registros.set(5_500, BillingRecord.from(new OrderCreatedEvent(
                UUID.randomUUID(), null, 1, new BigDecimal("1.00"), Instant.now())));

        assertThatThrownBy(() -> writer.write(registros)).isInstanceOf(RuntimeException.class);

        assertThat(contar()).isZero();
    }

    private long contar() {
        return jdbc.queryForObject("SELECT count(*) FROM billing_record", Long.class);
    }

    private static List<BillingRecord> registros(int quantidade) {
        var lista = new ArrayList<BillingRecord>(quantidade);
        for (int i = 0; i < quantidade; i++) {
            lista.add(registro(UUID.randomUUID(), "10.00", 1));
        }
        return lista;
    }

    private static BillingRecord registro(UUID id, String preco, int quantidade) {
        return BillingRecord.from(new OrderCreatedEvent(id, "teclado", quantidade, new BigDecimal(preco), Instant.now()));
    }
}
