package org.stevearmstrong.ledgerguard.demo.result;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationStatus;

class ReconciliationResultStoreTest {

    @Test
    void clearsTheLocalResultProjection() {
        ReconciliationResultStore store = new ReconciliationResultStore();
        store.receive(new ReconciliationResult(
                UUID.randomUUID(),
                "TX-DASHBOARD",
                UUID.randomUUID(),
                UUID.randomUUID(),
                ReconciliationStatus.MATCHED,
                new BigDecimal("125.50"),
                new BigDecimal("125.50"),
                "CAD",
                "CAD",
                Instant.parse("2026-08-16T12:00:00Z"),
                "Payment and ledger entry match"
        ));

        assertThat(store.findAll(null)).hasSize(1);

        store.clear();

        assertThat(store.findAll(null)).isEmpty();
    }
}
