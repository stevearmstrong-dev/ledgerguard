package org.stevearmstrong.ledgerguard.contracts;

import java.time.Instant;
import java.util.UUID;

public interface IdentifiedEvent {

    UUID eventId();

    String transactionId();

    Instant occurredAt();
}
