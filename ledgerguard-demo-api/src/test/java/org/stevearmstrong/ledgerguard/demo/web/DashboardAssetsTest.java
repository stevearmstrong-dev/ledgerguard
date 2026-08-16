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

        assertThat(html)
                .contains("LedgerGuard · Reconciliation Console")
                .contains("Follow the transaction")
                .contains("Recent reconciliations")
                .contains("/app.js")
                .contains("/styles.css");
        assertThat(javascript)
                .contains("/api/scenarios/")
                .contains("/api/reconciliations")
                .contains("waitForResult")
                .contains("MISSING_LEDGER_ENTRY")
                .contains("visibilitychange");
    }

    private String resource(String path) throws IOException {
        return new ClassPathResource(path).getContentAsString(StandardCharsets.UTF_8);
    }
}
