package org.stevearmstrong.ledgerguard.demo.scenario;

import java.util.concurrent.CompletableFuture;

import org.springframework.kafka.support.SendResult;

@FunctionalInterface
public interface ScenarioEventSender {

    CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object event);
}
