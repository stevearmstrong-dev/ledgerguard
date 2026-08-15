package org.stevearmstrong.ledgerguard.reconciliation.domain;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.stevearmstrong.ledgerguard.contracts.LedgerEntryEvent;
import org.stevearmstrong.ledgerguard.contracts.PaymentEvent;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationStatus;

class ReconciliationServiceTest {

    private static final Instant NOW = Instant.parse("2026-08-15T12:00:00Z");

    private final ReconciliationService service = new ReconciliationService(
            Clock.fixed(NOW, ZoneOffset.UTC)
    );

    @Test
    void matchesEquivalentPaymentAndLedgerEntry() {
        var result = service.reconcile(
                payment("125.50", "CAD"),
                ledgerEntry("125.50", "CAD")
        );

        assertThat(result.status()).isEqualTo(ReconciliationStatus.MATCHED);
        assertThat(result.evaluatedAt()).isEqualTo(NOW);
        assertThat(result.requiresReview()).isFalse();
    }

    @Test
    void detectsAmountMismatch() {
        var result = service.reconcile(
                payment("125.50", "CAD"),
                ledgerEntry("121.00", "CAD")
        );

        assertThat(result.status()).isEqualTo(ReconciliationStatus.AMOUNT_MISMATCH);
        assertThat(result.requiresReview()).isTrue();
    }

    @Test
    void detectsCurrencyMismatchBeforeComparingAmounts() {
        var result = service.reconcile(
                payment("125.50", "CAD"),
                ledgerEntry("125.50", "USD")
        );

        assertThat(result.status()).isEqualTo(ReconciliationStatus.CURRENCY_MISMATCH);
    }

    @Test
    void detectsMissingLedgerEntry() {
        var result = service.reconcile(payment("125.50", "CAD"), null);

        assertThat(result.status()).isEqualTo(ReconciliationStatus.MISSING_LEDGER_ENTRY);
        assertThat(result.ledgerEventId()).isNull();
    }

    @Test
    void detectsMissingPayment() {
        var result = service.reconcile(null, ledgerEntry("125.50", "CAD"));

        assertThat(result.status()).isEqualTo(ReconciliationStatus.MISSING_PAYMENT);
        assertThat(result.paymentEventId()).isNull();
    }

    @Test
    void classifiesDuplicateEvents() {
        assertThat(service.duplicatePayment(payment("125.50", "CAD")).status())
                .isEqualTo(ReconciliationStatus.DUPLICATE_PAYMENT);
        assertThat(service.duplicateLedgerEntry(ledgerEntry("125.50", "CAD")).status())
                .isEqualTo(ReconciliationStatus.DUPLICATE_LEDGER_ENTRY);
    }

    private PaymentEvent payment(String amount, String currency) {
        return new PaymentEvent(
                UUID.randomUUID(),
                "TX-TEST",
                new BigDecimal(amount),
                currency,
                "PSP-TEST",
                NOW
        );
    }

    private LedgerEntryEvent ledgerEntry(String amount, String currency) {
        return new LedgerEntryEvent(
                UUID.randomUUID(),
                "TX-TEST",
                new BigDecimal(amount),
                currency,
                "GL-TEST",
                NOW.plusMillis(500)
        );
    }
}
