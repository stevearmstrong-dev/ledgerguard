package org.stevearmstrong.ledgerguard.reconciliation.config;

import java.time.Duration;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledgerguard.reconciliation")
public record ReconciliationProperties(
        Duration joinWindow,
        Duration gracePeriod,
        Duration deduplicationRetention
) {
}
