package org.stevearmstrong.ledgerguard.demo.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;

class DashboardAssetsTest {

    @Test
    void dashboardExposesTheRealScenarioAndResultEndpoints() throws IOException {
        String html = resource("static/index.html");
        String javascript = resource("static/app.js");
        String applicationConfiguration = resource("application.yml");

        assertThat(html)
                .contains("LedgerGuard · Reconciliation Console")
                .contains("<em class=\"brand-byline\">by Steve Armstrong</em>")
                .contains("Transaction workbench")
                .contains("Follow the transaction")
                .contains("LIVE PROCESSING TRACE")
                .contains("Recent transaction activity")
                .contains("Transactions")
                .contains("Flagged")
                .contains("/app.js?v=2")
                .contains("/styles.css?v=4");
        assertThat(javascript)
                .contains("/api/scenarios/")
                .contains("/api/transactions")
                .contains("/api/reconciliations")
                .contains("waitForResult")
                .contains("setTraceStep")
                .contains("groupResultsByTransaction")
                .contains("Duplicate payment ignored")
                .contains("MISSING_LEDGER_ENTRY")
                .contains("visibilitychange");
        assertThat(applicationConfiguration).contains("no-cache: true");
    }

    private String resource(String path) throws IOException {
        return new ClassPathResource(path).getContentAsString(StandardCharsets.UTF_8);
    }
}
