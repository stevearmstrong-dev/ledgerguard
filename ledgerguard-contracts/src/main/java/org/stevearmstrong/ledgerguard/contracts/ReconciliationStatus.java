package org.stevearmstrong.ledgerguard.contracts;

public enum ReconciliationStatus {
    MATCHED,
    AMOUNT_MISMATCH,
    CURRENCY_MISMATCH,
    MISSING_LEDGER_ENTRY,
    MISSING_PAYMENT,
    DUPLICATE_PAYMENT,
    DUPLICATE_LEDGER_ENTRY
}
