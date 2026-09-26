package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import dev.marcosjesus.orderevents.support.AbstractPostgresIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * O faturamento consome em lotes e grava cada lote numa transação. Com o tamanho de lote
 * reduzido para 500, 3.000 eventos só cabem em vários lotes, e nenhum pode passar de 500.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE, properties = "order-events.billing.batch-size=500")
@EmbeddedKafka(partitions = 3, topics = {KafkaTopics.ORDER_CREATED, KafkaTopics.ORDER_CREATED_DLT})
class BillingBatchIntegrationTest extends AbstractPostgresIntegrationTest {

    private static final int EVENTOS = 3_000;
    private static final int TAMANHO_DO_LOTE = 500;

    @Autowired
    private KafkaTemplate<Object, Object> kafkaTemplate;

    @Autowired
    private BillingRecordRepository repository;

    @Autowired
    private MeterRegistry registry;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> billingBatchContainerFactory;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void gravaTodosOsEventosEmLotesQueNaoPassamDoTamanhoConfigurado() {
        IntStream.range(0, EVENTOS).forEach(i -> {
            var id = UUID.randomUUID();
            kafkaTemplate.send(KafkaTopics.ORDER_CREATED, id.toString(),
                    new OrderCreatedEvent(id, "teclado", 1 + i % 5, new BigDecimal("10.00"), Instant.now()));
        });

        await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(repository.count()).isEqualTo(EVENTOS));

        var tamanhos = registry.get("orders.billing.batch.size").summary();
        assertThat(tamanhos.max()).isLessThanOrEqualTo(TAMANHO_DO_LOTE).isGreaterThan(1);
        assertThat(tamanhos.totalAmount()).isEqualTo(EVENTOS);
        assertThat(registry.get("orders.billing.persisted").counter().count()).isEqualTo(EVENTOS);
    }

    @Test
    void maxPollRecordsDoConsumerEstaAlinhadoComOTamanhoDoLoteNoPostgres() {
        var props = billingBatchContainerFactory.getConsumerFactory().getConfigurationProperties();

        assertThat(props.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG)).isEqualTo(TAMANHO_DO_LOTE);
    }

    @Test
    void driverReescreveOBatchEmInsertMultiLinha() throws Exception {
        var hikari = dataSource.unwrap(HikariDataSource.class);

        assertThat(hikari.getDataSourceProperties().getProperty("reWriteBatchedInserts")).isEqualTo("true");
    }
}
