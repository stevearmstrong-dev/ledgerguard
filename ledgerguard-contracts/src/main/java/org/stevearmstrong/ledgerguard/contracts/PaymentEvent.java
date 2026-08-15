package org.stevearmstrong.ledgerguard.contracts;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;
import java.util.Objects;
import java.util.UUID;

public record PaymentEvent(
        UUID eventId,
        String transactionId,
        BigDecimal amount,
        String currency,
        String processorReference,
        Instant occurredAt
) implements IdentifiedEvent {

    public PaymentEvent {
        Objects.requireNonNull(eventId, "eventId is required");
        transactionId = requireText(transactionId, "transactionId");
        Objects.requireNonNull(amount, "amount is required");
        if (amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be greater than zero");
        }
        currency = requireCurrency(currency);
        processorReference = requireText(processorReference, "processorReference");
        Objects.requireNonNull(occurredAt, "occurredAt is required");
    }

    private static String requireCurrency(String value) {
        String currency = requireText(value, "currency").toUpperCase(Locale.ROOT);
        if (currency.length() != 3) {
            throw new IllegalArgumentException("currency must be a three-letter ISO code");
        }
        return currency;
    }

    private static String requireText(String value, String field) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(field + " is required");
        }
        return value;
    }
}
