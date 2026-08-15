package org.stevearmstrong.ledgerguard.demo.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;
import org.stevearmstrong.ledgerguard.demo.config.LedgerGuardTopics;

class ScenarioPublisherTest {

    private RecordingEventSender eventSender;
    private ScenarioPublisher publisher;

    @BeforeEach
    void setUp() {
        eventSender = new RecordingEventSender();
        publisher = new ScenarioPublisher(
                eventSender,
                new LedgerGuardTopics("payments", "ledger", "results", "exceptions"),
                Clock.fixed(Instant.parse("2026-08-15T12:00:00Z"), ZoneOffset.UTC)
        );
    }

    @Test
    void publishesBothSidesOfAMatchedScenario() {
        ScenarioResponse response = publisher.publish(ScenarioType.MATCHED);

        assertThat(response.publishedEvents()).hasSize(2);
        assertThat(eventSender.events()).hasSize(2);
    }

    @Test
    void publishesTheSamePaymentTwiceForDuplicateScenario() {
        ScenarioResponse response = publisher.publish(ScenarioType.DUPLICATE_PAYMENT);

        assertThat(response.publishedEvents())
                .hasSize(3)
                .satisfies(events -> assertThat(events.get(0)).isEqualTo(events.get(1)));
        assertThat(eventSender.events()).hasSize(3);
    }

    private static final class RecordingEventSender implements ScenarioEventSender {

        private final List<Object> events = new ArrayList<>();

        @Override
        public CompletableFuture<SendResult<String, Object>> send(String topic, String key, Object event) {
            events.add(event);
            return CompletableFuture.completedFuture(null);
        }

        List<Object> events() {
            return List.copyOf(events);
        }
    }
}
