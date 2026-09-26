package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.CreateOrderRequest;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import dev.marcosjesus.orderevents.support.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Sobe a aplicação inteira (controller, producer, os 3 listeners e um Postgres real via
 * Testcontainers) e confirma que criar um pedido via HTTP resulta num BillingRecord
 * persistido com o faturamento (quantidade x preço unitário) calculado corretamente.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDER_CREATED, KafkaTopics.ORDER_CREATED_DLT})
class BillingPersistenceIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private BillingRecordRepository repository;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void persisteOFaturamentoAoCriarUmPedido() {
        var request = new CreateOrderRequest("mouse-gamer", 4, new BigDecimal("80.00"));

        ResponseEntity<OrderCreatedEvent> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders", request, OrderCreatedEvent.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        var orderId = response.getBody().orderId();

        await().atMost(10, SECONDS).untilAsserted(() -> {
            var saved = repository.findById(orderId);
            assertThat(saved).isPresent();
            assertThat(saved.get().getProduct()).isEqualTo("mouse-gamer");
            assertThat(saved.get().getQuantity()).isEqualTo(4);
            assertThat(saved.get().getUnitPrice()).isEqualByComparingTo("80.00");
            assertThat(saved.get().getTotalAmount()).isEqualByComparingTo("320.00");
        });
    }
}
