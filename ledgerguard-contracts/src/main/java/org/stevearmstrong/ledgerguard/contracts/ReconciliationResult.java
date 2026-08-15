package org.stevearmstrong.ledgerguard.contracts;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

public record ReconciliationResult(
        UUID reconciliationId,
        String transactionId,
        UUID paymentEventId,
        UUID ledgerEventId,
        ReconciliationStatus status,
        BigDecimal paymentAmount,
        BigDecimal ledgerAmount,
        String paymentCurrency,
        String ledgerCurrency,
        Instant evaluatedAt,
        String reason
) {

    public ReconciliationResult {
        Objects.requireNonNull(reconciliationId, "reconciliationId is required");
        if (transactionId == null || transactionId.isBlank()) {
            throw new IllegalArgumentException("transactionId is required");
        }
        Objects.requireNonNull(status, "status is required");
        Objects.requireNonNull(evaluatedAt, "evaluatedAt is required");
        if (reason == null || reason.isBlank()) {
            throw new IllegalArgumentException("reason is required");
        }
    }

    public boolean requiresReview() {
        return status != ReconciliationStatus.MATCHED;
    }
}
