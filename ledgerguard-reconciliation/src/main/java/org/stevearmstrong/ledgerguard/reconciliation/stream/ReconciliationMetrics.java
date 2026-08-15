package org.stevearmstrong.ledgerguard.reconciliation.stream;

import java.util.EnumMap;
import java.util.Map;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationStatus;

@Component
public class ReconciliationMetrics {

    private final Map<ReconciliationStatus, Counter> counters;

    public ReconciliationMetrics(MeterRegistry meterRegistry) {
        counters = new EnumMap<>(ReconciliationStatus.class);
        for (ReconciliationStatus status : ReconciliationStatus.values()) {
            counters.put(status, Counter.builder("ledgerguard.reconciliations")
                    .description("Reconciliation outcomes processed by LedgerGuard")
                    .tag("status", status.name())
                    .register(meterRegistry));
        }
    }

    public void record(ReconciliationResult result) {
        counters.get(result.status()).increment();
    }
}
