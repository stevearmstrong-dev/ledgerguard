package org.stevearmstrong.ledgerguard.demo.scenario;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.math.BigDecimal;
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
        SubmissionResponse response = publisher.publish(ScenarioType.MATCHED);

        assertThat(response.publishedEvents())
                .extracting(PublishedEvent::eventType)
                .containsExactly("PAYMENT", "LEDGER_ENTRY");
        assertThat(eventSender.events()).hasSize(2);
    }

    @Test
    void publishesTheSamePaymentTwiceForDuplicateScenario() {
        SubmissionResponse response = publisher.publish(ScenarioType.DUPLICATE_PAYMENT);

        assertThat(response.publishedEvents())
                .hasSize(3)
                .satisfies(events -> {
                    assertThat(events.get(0).eventId()).isEqualTo(events.get(1).eventId());
                    assertThat(events.get(0).duplicate()).isFalse();
                    assertThat(events.get(1).duplicate()).isTrue();
                });
        assertThat(eventSender.events()).hasSize(3);
    }

    @Test
    void publishesRecruiterInputInTheRequestedOrder() {
        SubmissionResponse response = publisher.publish(new TransactionRequest(
                "demo-recruiter-01",
                new BigDecimal("200.00"),
                "cad",
                new BigDecimal("198.50"),
                "cad",
                EventOrder.LEDGER_FIRST,
                0,
                false
        ));

        assertThat(response.transactionId()).isEqualTo("DEMO-RECRUITER-01");
        assertThat(response.runType()).isEqualTo("CUSTOM");
        assertThat(response.publishedEvents())
                .extracting(PublishedEvent::eventType)
                .containsExactly("LEDGER_ENTRY", "PAYMENT");
        assertThat(response.publishedEvents())
                .extracting(PublishedEvent::amount)
                .containsExactly(new BigDecimal("198.50"), new BigDecimal("200.00"));
    }

    @Test
    void rejectsACustomRequestWithoutEvents() {
        TransactionRequest request = new TransactionRequest(
                "demo-empty",
                null,
                null,
                null,
                null,
                EventOrder.PAYMENT_FIRST,
                0,
                false
        );

        assertThatThrownBy(() -> publisher.publish(request))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Publish at least one payment or ledger event");
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
