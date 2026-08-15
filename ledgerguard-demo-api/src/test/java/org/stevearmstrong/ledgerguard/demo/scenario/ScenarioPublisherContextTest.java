package org.stevearmstrong.ledgerguard.demo.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.stevearmstrong.ledgerguard.demo.config.LedgerGuardTopics;

class ScenarioPublisherContextTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withBean(
                    ScenarioEventSender.class,
                    () -> (topic, key, event) -> CompletableFuture.completedFuture(null)
            )
            .withBean(
                    LedgerGuardTopics.class,
                    () -> new LedgerGuardTopics("payments", "ledger", "results", "exceptions")
            )
            .withBean(ScenarioPublisher.class);

    @Test
    void selectsTheProductionConstructor() {
        contextRunner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(ScenarioPublisher.class);
        });
    }
}
