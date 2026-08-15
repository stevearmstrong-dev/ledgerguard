package org.stevearmstrong.ledgerguard.reconciliation.domain;

import java.time.Clock;
import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.stevearmstrong.ledgerguard.contracts.LedgerEntryEvent;
import org.stevearmstrong.ledgerguard.contracts.PaymentEvent;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationStatus;

@Service
public class ReconciliationService {

    private final Clock clock;

    public ReconciliationService() {
        this(Clock.systemUTC());
    }

    ReconciliationService(Clock clock) {
        this.clock = clock;
    }

    public ReconciliationResult reconcile(PaymentEvent payment, LedgerEntryEvent ledgerEntry) {
        if (payment == null) {
            return result(
                    ledgerEntry.transactionId(),
                    null,
                    ledgerEntry,
                    ReconciliationStatus.MISSING_PAYMENT,
                    "A ledger entry arrived without a corresponding payment event"
            );
        }

        if (ledgerEntry == null) {
            return result(
                    payment.transactionId(),
                    payment,
                    null,
                    ReconciliationStatus.MISSING_LEDGER_ENTRY,
                    "A payment event arrived without a corresponding ledger entry"
            );
        }

        if (!payment.currency().equals(ledgerEntry.currency())) {
            return result(
                    payment.transactionId(),
                    payment,
                    ledgerEntry,
                    ReconciliationStatus.CURRENCY_MISMATCH,
                    "Payment and ledger currencies do not match"
            );
        }

        if (payment.amount().compareTo(ledgerEntry.amount()) != 0) {
            return result(
                    payment.transactionId(),
                    payment,
                    ledgerEntry,
                    ReconciliationStatus.AMOUNT_MISMATCH,
                    "Payment and ledger amounts do not match"
            );
        }

        return result(
                payment.transactionId(),
                payment,
                ledgerEntry,
                ReconciliationStatus.MATCHED,
                "Payment and ledger entry match"
        );
    }

    public ReconciliationResult duplicatePayment(PaymentEvent payment) {
        return result(
                payment.transactionId(),
                payment,
                null,
                ReconciliationStatus.DUPLICATE_PAYMENT,
                "The payment event ID has already been processed"
        );
    }

    public ReconciliationResult duplicateLedgerEntry(LedgerEntryEvent ledgerEntry) {
        return result(
                ledgerEntry.transactionId(),
                null,
                ledgerEntry,
                ReconciliationStatus.DUPLICATE_LEDGER_ENTRY,
                "The ledger event ID has already been processed"
        );
    }

    private ReconciliationResult result(
            String transactionId,
            PaymentEvent payment,
            LedgerEntryEvent ledgerEntry,
            ReconciliationStatus status,
            String reason
    ) {
        Instant evaluatedAt = clock.instant();
        return new ReconciliationResult(
                UUID.randomUUID(),
                transactionId,
                payment == null ? null : payment.eventId(),
                ledgerEntry == null ? null : ledgerEntry.eventId(),
                status,
                payment == null ? null : payment.amount(),
                ledgerEntry == null ? null : ledgerEntry.amount(),
                payment == null ? null : payment.currency(),
                ledgerEntry == null ? null : ledgerEntry.currency(),
                evaluatedAt,
                reason
        );
    }
}
