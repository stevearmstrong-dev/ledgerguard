package org.stevearmstrong.ledgerguard.demo.scenario;

import java.time.Instant;
import java.util.List;

public record SubmissionResponse(
        String transactionId,
        String runType,
        Instant submittedAt,
        EventOrder eventOrder,
        long eventDelayMs,
        List<PublishedEvent> publishedEvents
) {
}
