package org.stevearmstrong.ledgerguard.reconciliation.stream;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.stevearmstrong.ledgerguard.contracts.LedgerEntryEvent;
import org.stevearmstrong.ledgerguard.contracts.PaymentEvent;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationStatus;
import org.stevearmstrong.ledgerguard.reconciliation.config.LedgerGuardTopics;
import org.stevearmstrong.ledgerguard.reconciliation.config.ReconciliationProperties;
import org.stevearmstrong.ledgerguard.reconciliation.domain.ReconciliationService;

class ReconciliationTopologyTest {

    private static final Instant EVENT_TIME = Instant.parse("2026-08-15T12:00:00Z");
    private static final LedgerGuardTopics TOPICS = new LedgerGuardTopics(
            "payments.test",
            "ledger-entries.test",
            "reconciliations.test",
            "exceptions.test"
    );

    private TopologyTestDriver driver;
    private TestInputTopic<String, PaymentEvent> payments;
    private TestInputTopic<String, LedgerEntryEvent> ledgerEntries;
    private TestOutputTopic<String, ReconciliationResult> reconciliations;
    private TestOutputTopic<String, ReconciliationResult> exceptions;

    @BeforeEach
    void setUp() {
        var builder = new org.apache.kafka.streams.StreamsBuilder();
        var topology = new ReconciliationTopology();
        topology.reconciliationStream(
                builder,
                TOPICS,
                new ReconciliationProperties(Duration.ofSeconds(10), Duration.ofSeconds(3), Duration.ofHours(24)),
                new ReconciliationService(),
                new ReconciliationMetrics(new SimpleMeterRegistry())
        );

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ledgerguard-topology-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "unused:9092");
        properties.put(StreamsConfig.STATE_DIR_CONFIG, "target/kafka-streams-test");
        driver = new TopologyTestDriver(builder.build(), properties);

        var paymentSerde = new JacksonJsonSerde<>(PaymentEvent.class);
        var ledgerSerde = new JacksonJsonSerde<>(LedgerEntryEvent.class);
        var resultSerde = new JacksonJsonSerde<>(ReconciliationResult.class);

        payments = driver.createInputTopic(
                TOPICS.payments(),
                Serdes.String().serializer(),
                paymentSerde.serializer()
        );
        ledgerEntries = driver.createInputTopic(
                TOPICS.ledgerEntries(),
                Serdes.String().serializer(),
                ledgerSerde.serializer()
        );
        reconciliations = driver.createOutputTopic(
                TOPICS.reconciliations(),
                Serdes.String().deserializer(),
                resultSerde.deserializer()
        );
        exceptions = driver.createOutputTopic(
                TOPICS.exceptions(),
                Serdes.String().deserializer(),
                resultSerde.deserializer()
        );
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void reconcilesMatchingEvents() {
        PaymentEvent payment = payment(UUID.randomUUID(), "TX-MATCH", "125.50", "CAD");
        LedgerEntryEvent ledgerEntry = ledgerEntry(UUID.randomUUID(), "TX-MATCH", "125.50", "CAD");

        payments.pipeInput(payment.transactionId(), payment, EVENT_TIME);
        ledgerEntries.pipeInput(ledgerEntry.transactionId(), ledgerEntry, EVENT_TIME.plusMillis(500));

        ReconciliationResult result = reconciliations.readValue();
        assertThat(result.status()).isEqualTo(ReconciliationStatus.MATCHED);
        assertThat(exceptions.isEmpty()).isTrue();
    }

    @Test
    void routesMismatchesToTheExceptionTopic() {
        PaymentEvent payment = payment(UUID.randomUUID(), "TX-MISMATCH", "125.50", "CAD");
        LedgerEntryEvent ledgerEntry = ledgerEntry(UUID.randomUUID(), "TX-MISMATCH", "121.00", "CAD");

        payments.pipeInput(payment.transactionId(), payment, EVENT_TIME);
        ledgerEntries.pipeInput(ledgerEntry.transactionId(), ledgerEntry, EVENT_TIME.plusMillis(500));

        assertThat(reconciliations.readValue().status())
                .isEqualTo(ReconciliationStatus.AMOUNT_MISMATCH);
        assertThat(exceptions.readValue().status())
                .isEqualTo(ReconciliationStatus.AMOUNT_MISMATCH);
    }

    @Test
    void detectsDuplicatePaymentEventIdsWithoutJoiningThemTwice() {
        UUID eventId = UUID.randomUUID();
        PaymentEvent payment = payment(eventId, "TX-DUPLICATE", "125.50", "CAD");
        LedgerEntryEvent ledgerEntry = ledgerEntry(UUID.randomUUID(), "TX-DUPLICATE", "125.50", "CAD");

        payments.pipeInput(payment.transactionId(), payment, EVENT_TIME);
        payments.pipeInput(payment.transactionId(), payment, EVENT_TIME.plusMillis(100));
        ledgerEntries.pipeInput(ledgerEntry.transactionId(), ledgerEntry, EVENT_TIME.plusMillis(500));

        List<ReconciliationStatus> statuses = reconciliations.readValuesToList().stream()
                .map(ReconciliationResult::status)
                .toList();
        assertThat(statuses)
                .containsExactlyInAnyOrder(
                        ReconciliationStatus.DUPLICATE_PAYMENT,
                        ReconciliationStatus.MATCHED
                );
        assertThat(exceptions.readValue().status())
                .isEqualTo(ReconciliationStatus.DUPLICATE_PAYMENT);
    }

    @Test
    void emitsMissingLedgerEntryAfterTheEventTimeWindowCloses() {
        PaymentEvent unmatchedPayment = payment(
                UUID.randomUUID(),
                "TX-MISSING-LEDGER",
                "125.50",
                "CAD"
        );
        Instant afterWindow = EVENT_TIME.plusSeconds(14);
        PaymentEvent laterUnrelatedPayment = new PaymentEvent(
                UUID.randomUUID(),
                "TX-LATER-PAYMENT",
                new BigDecimal("42.00"),
                "CAD",
                "PSP-LATER",
                afterWindow
        );
        LedgerEntryEvent laterUnrelatedEntry = new LedgerEntryEvent(
                UUID.randomUUID(),
                "TX-LATER-LEDGER",
                new BigDecimal("42.00"),
                "CAD",
                "GL-LATER",
                afterWindow
        );

        payments.pipeInput(unmatchedPayment.transactionId(), unmatchedPayment, EVENT_TIME);
        payments.pipeInput(
                laterUnrelatedPayment.transactionId(),
                laterUnrelatedPayment,
                afterWindow
        );
        ledgerEntries.pipeInput(
                laterUnrelatedEntry.transactionId(),
                laterUnrelatedEntry,
                afterWindow
        );

        assertThat(reconciliations.readValuesToList())
                .extracting(ReconciliationResult::status)
                .contains(ReconciliationStatus.MISSING_LEDGER_ENTRY);
        assertThat(exceptions.readValuesToList())
                .extracting(ReconciliationResult::status)
                .contains(ReconciliationStatus.MISSING_LEDGER_ENTRY);
    }

    private PaymentEvent payment(UUID eventId, String transactionId, String amount, String currency) {
        return new PaymentEvent(
                eventId,
                transactionId,
                new BigDecimal(amount),
                currency,
                "PSP-TEST",
                EVENT_TIME
        );
    }

    private LedgerEntryEvent ledgerEntry(UUID eventId, String transactionId, String amount, String currency) {
        return new LedgerEntryEvent(
                eventId,
                transactionId,
                new BigDecimal(amount),
                currency,
                "GL-TEST",
                EVENT_TIME.plusMillis(500)
        );
    }
}
