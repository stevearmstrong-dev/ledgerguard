package org.stevearmstrong.ledgerguard.reconciliation.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

@Configuration(proxyBeanMethods = false)
public class TopicConfiguration {

    @Bean
    KafkaAdmin.NewTopics ledgerGuardTopics(LedgerGuardTopics topics) {
        return new KafkaAdmin.NewTopics(
                topic(topics.payments()),
                topic(topics.ledgerEntries()),
                topic(topics.reconciliations()),
                topic(topics.exceptions())
        );
    }

    private NewTopic topic(String name) {
        return TopicBuilder.name(name)
                .partitions(3)
                .replicas(1)
                .build();
    }
}
