package dev.marcosjesus.orderevents.order;

import dev.marcosjesus.orderevents.kafka.KafkaTopics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Sobe a aplicação inteira (controller, producer e os listeners reais de
 * inventory/notification) contra um broker Kafka embarcado, e usa um terceiro
 * consumer group dedicado só ao teste para confirmar que o evento publicado
 * pelo controller efetivamente chega ao tópico e pode ser consumido de volta.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(partitions = 1, topics = {KafkaTopics.ORDER_CREATED, KafkaTopics.ORDER_CREATED_DLT})
class OrderCreationIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestOrderCreatedCollector collector;

    @LocalServerPort
    private int port;

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers"));
    }

    @Test
    void publicaOEventoNoKafkaAoCriarUmPedido() throws InterruptedException {
        var request = new CreateOrderRequest("teclado-mecanico", 2);

        ResponseEntity<OrderCreatedEvent> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders", request, OrderCreatedEvent.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        var published = response.getBody();
        assertThat(published).isNotNull();
        assertThat(published.product()).isEqualTo("teclado-mecanico");
        assertThat(published.quantity()).isEqualTo(2);

        OrderCreatedEvent receivedFromTopic = collector.queue().poll(10, TimeUnit.SECONDS);

        assertThat(receivedFromTopic).isEqualTo(published);
    }

    @TestConfiguration
    static class TestListenersConfig {

        @Bean
        TestOrderCreatedCollector testOrderCreatedCollector() {
            return new TestOrderCreatedCollector();
        }
    }

    static class TestOrderCreatedCollector {

        private final BlockingQueue<OrderCreatedEvent> queue = new LinkedBlockingQueue<>();

        @KafkaListener(topics = KafkaTopics.ORDER_CREATED, groupId = "test-verifier")
        void capture(OrderCreatedEvent event) {
            queue.add(event);
        }

        BlockingQueue<OrderCreatedEvent> queue() {
            return queue;
        }
    }
}
