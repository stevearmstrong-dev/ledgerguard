package org.stevearmstrong.ledgerguard.demo.scenario;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

@Component
public class KafkaScenarioEventSender implements ScenarioEventSender {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaScenarioEventSender(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object event) {
        return kafkaTemplate.send(topic, key, event);
    }
}
