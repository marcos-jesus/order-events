package dev.marcosjesus.orderevents.kafka;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic orderCreatedTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_CREATED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderCreatedDeadLetterTopic() {
        return TopicBuilder.name(KafkaTopics.ORDER_CREATED_DLT)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
