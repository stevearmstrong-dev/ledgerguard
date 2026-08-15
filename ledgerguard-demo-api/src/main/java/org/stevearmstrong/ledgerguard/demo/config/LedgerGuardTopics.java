package org.stevearmstrong.ledgerguard.demo.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "ledgerguard.topics")
public record LedgerGuardTopics(
        String payments,
        String ledgerEntries,
        String reconciliations,
        String exceptions
) {
}
