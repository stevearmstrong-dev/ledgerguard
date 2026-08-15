package org.stevearmstrong.ledgerguard.demo.scenario;

import java.time.Instant;
import java.util.List;

public record ScenarioResponse(
        String transactionId,
        ScenarioType scenario,
        Instant submittedAt,
        List<String> publishedEvents
) {
}
