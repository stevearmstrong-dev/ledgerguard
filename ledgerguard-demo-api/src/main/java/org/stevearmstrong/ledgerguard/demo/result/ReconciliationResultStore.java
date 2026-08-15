package org.stevearmstrong.ledgerguard.demo.result;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;

@Component
public class ReconciliationResultStore {

    private static final int MAX_RESULTS = 200;

    private final ConcurrentLinkedDeque<ReconciliationResult> results = new ConcurrentLinkedDeque<>();

    @KafkaListener(topics = "${ledgerguard.topics.reconciliations}")
    public void receive(ReconciliationResult result) {
        results.addFirst(result);
        while (results.size() > MAX_RESULTS) {
            results.pollLast();
        }
    }

    public List<ReconciliationResult> findAll(String transactionId) {
        return results.stream()
                .filter(result -> transactionId == null || result.transactionId().equals(transactionId))
                .sorted(Comparator.comparing(ReconciliationResult::evaluatedAt).reversed())
                .toList();
    }
}
