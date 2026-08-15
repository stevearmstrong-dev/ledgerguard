package org.stevearmstrong.ledgerguard.demo.scenario;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.stevearmstrong.ledgerguard.contracts.LedgerEntryEvent;
import org.stevearmstrong.ledgerguard.contracts.PaymentEvent;
import org.stevearmstrong.ledgerguard.demo.config.LedgerGuardTopics;

@Service
public class ScenarioPublisher {

    private static final BigDecimal BASE_AMOUNT = new BigDecimal("125.50");

    private final ScenarioEventSender eventSender;
    private final LedgerGuardTopics topics;
    private final Clock clock;

    @Autowired
    public ScenarioPublisher(ScenarioEventSender eventSender, LedgerGuardTopics topics) {
        this(eventSender, topics, Clock.systemUTC());
    }

    ScenarioPublisher(ScenarioEventSender eventSender, LedgerGuardTopics topics, Clock clock) {
        this.eventSender = eventSender;
        this.topics = topics;
        this.clock = clock;
    }

    public ScenarioResponse publish(ScenarioType scenario) {
        Instant now = clock.instant();
        String transactionId = "TX-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        PaymentEvent payment = payment(transactionId, BASE_AMOUNT, "CAD", now);
        LedgerEntryEvent ledgerEntry = ledgerEntry(transactionId, BASE_AMOUNT, "CAD", now.plusMillis(500));
        List<String> publishedEvents = new ArrayList<>();
        List<CompletableFuture<SendResult<String, Object>>> sends = new ArrayList<>();

        switch (scenario) {
            case MATCHED -> {
                publishPayment(payment, sends, publishedEvents);
                publishLedgerEntry(ledgerEntry, sends, publishedEvents);
            }
            case AMOUNT_MISMATCH -> {
                publishPayment(payment, sends, publishedEvents);
                publishLedgerEntry(
                        ledgerEntry(transactionId, new BigDecimal("121.00"), "CAD", now.plusMillis(500)),
                        sends,
                        publishedEvents
                );
            }
            case CURRENCY_MISMATCH -> {
                publishPayment(payment, sends, publishedEvents);
                publishLedgerEntry(
                        ledgerEntry(transactionId, BASE_AMOUNT, "USD", now.plusMillis(500)),
                        sends,
                        publishedEvents
                );
            }
            case MISSING_LEDGER_ENTRY -> publishPayment(payment, sends, publishedEvents);
            case MISSING_PAYMENT -> publishLedgerEntry(ledgerEntry, sends, publishedEvents);
            case DUPLICATE_PAYMENT -> {
                publishPayment(payment, sends, publishedEvents);
                publishPayment(payment, sends, publishedEvents);
                publishLedgerEntry(ledgerEntry, sends, publishedEvents);
            }
            case OUT_OF_ORDER_MATCH -> {
                publishLedgerEntry(ledgerEntry, sends, publishedEvents);
                publishPayment(payment, sends, publishedEvents);
            }
        }

        awaitBrokerAcknowledgements(sends);
        return new ScenarioResponse(transactionId, scenario, now, List.copyOf(publishedEvents));
    }

    private void publishPayment(
            PaymentEvent event,
            List<CompletableFuture<SendResult<String, Object>>> sends,
            List<String> publishedEvents
    ) {
        sends.add(eventSender.send(topics.payments(), event.transactionId(), event));
        publishedEvents.add("PAYMENT:" + event.eventId());
    }

    private void publishLedgerEntry(
            LedgerEntryEvent event,
            List<CompletableFuture<SendResult<String, Object>>> sends,
            List<String> publishedEvents
    ) {
        sends.add(eventSender.send(topics.ledgerEntries(), event.transactionId(), event));
        publishedEvents.add("LEDGER_ENTRY:" + event.eventId());
    }

    private void awaitBrokerAcknowledgements(List<CompletableFuture<SendResult<String, Object>>> sends) {
        try {
            CompletableFuture.allOf(sends.toArray(CompletableFuture[]::new))
                    .get(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception exception) {
            throw new IllegalStateException("Kafka did not acknowledge the scenario events", exception);
        }
    }

    private PaymentEvent payment(
            String transactionId,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) {
        return new PaymentEvent(
                UUID.randomUUID(),
                transactionId,
                amount,
                currency,
                "PSP-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                occurredAt
        );
    }

    private LedgerEntryEvent ledgerEntry(
            String transactionId,
            BigDecimal amount,
            String currency,
            Instant occurredAt
    ) {
        return new LedgerEntryEvent(
                UUID.randomUUID(),
                transactionId,
                amount,
                currency,
                "GL-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(),
                occurredAt
        );
    }
}
