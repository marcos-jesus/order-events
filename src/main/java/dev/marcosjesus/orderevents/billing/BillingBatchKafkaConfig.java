package dev.marcosjesus.orderevents.billing;

import dev.marcosjesus.orderevents.order.OrderCreatedEvent;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.MicrometerConsumerListener;
import org.springframework.kafka.listener.CommonErrorHandler;

import java.util.Map;

/**
 * Container factory só do faturamento: entrega os eventos em lote (List) ao listener, com o
 * max.poll.records igual ao tamanho do batch gravado no Postgres. O inventory e o notification
 * continuam na factory padrão, um evento por vez.
 */
@Configuration
@EnableConfigurationProperties(BillingProperties.class)
public class BillingBatchKafkaConfig {

    /** Um lote de 5.000 eventos (~250 B cada) passa de 1 MB, o padrão de fetch por partição. */
    private static final int MAX_PARTITION_FETCH_BYTES = 4 * 1024 * 1024;

    /** Uma thread por partição do tópico order-created. */
    private static final int CONCURRENCY = 3;

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent> billingBatchContainerFactory(
            KafkaProperties kafkaProperties,
            BillingProperties billing,
            CommonErrorHandler kafkaErrorHandler,
            MeterRegistry meterRegistry) {
        Map<String, Object> props = kafkaProperties.buildConsumerProperties(null);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, billing.batchSize());
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, MAX_PARTITION_FETCH_BYTES);

        var consumerFactory = new DefaultKafkaConsumerFactory<String, OrderCreatedEvent>(props);
        consumerFactory.addListener(new MicrometerConsumerListener<>(meterRegistry));

        var factory = new ConcurrentKafkaListenerContainerFactory<String, OrderCreatedEvent>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(true);
        factory.setConcurrency(CONCURRENCY);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        // Observation e o timer legado registram spring.kafka.listener com rótulos diferentes e o
        // Prometheus descarta o segundo: com os dois ligados, os timers de inventory/notification somem.
        factory.getContainerProperties().setObservationEnabled(true);
        factory.getContainerProperties().setMicrometerEnabled(false);
        return factory;
    }
}
