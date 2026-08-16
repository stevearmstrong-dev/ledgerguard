package org.stevearmstrong.ledgerguard.demo.scenario;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PublishedEvent(
        int sequence,
        String eventType,
        String topic,
        UUID eventId,
        String transactionId,
        BigDecimal amount,
        String currency,
        String reference,
        Instant occurredAt,
        boolean duplicate
) {
}
