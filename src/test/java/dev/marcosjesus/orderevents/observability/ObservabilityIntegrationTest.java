package dev.marcosjesus.orderevents.observability;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import dev.marcosjesus.orderevents.order.CreateOrderRequest;
import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import dev.marcosjesus.orderevents.support.AbstractPostgresIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.math.BigDecimal;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Garante o contrato que o Zabbix consome: o endpoint Prometheus expõe requests HTTP,
 * o processamento de cada consumer group e o lag do Kafka. @AutoConfigureObservability
 * é necessário porque, em @SpringBootTest, os exporters de métricas ficam desligados.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureObservability
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDER_CREATED, KafkaTopics.ORDER_CREATED_DLT})
class ObservabilityIntegrationTest extends AbstractPostgresIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void expoeMetricasDeRequestsConsumersELagNoEndpointPrometheus() {
        restTemplate.postForEntity("/orders",
                new CreateOrderRequest("teclado-mecanico", 1, new BigDecimal("10.00")), OrderCreatedEvent.class);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            String scrape = restTemplate.getForObject("/actuator/prometheus", String.class);

            assertThat(scrape).contains("http_server_requests_seconds_count");
            assertThat(scrape).contains("spring_kafka_listener_seconds_count");
            assertThat(scrape).contains("inventory-service", "notification-service", "billing-service");
            assertThat(scrape).contains("kafka_consumer_fetch_manager_records_lag_max");
            assertThat(scrape).contains("hikaricp_connections_active");
            assertThat(scrape).contains("orders_dead_lettered_total");
        });
    }

    @Test
    void naoExpoeEndpointsSensiveisDoActuator() {
        var response = restTemplate.getForEntity("/actuator/env", String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
    }
}
