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

    public SubmissionResponse publish(ScenarioType scenario) {
        Instant now = clock.instant();
        String transactionId = "TX-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        PaymentEvent payment = payment(transactionId, BASE_AMOUNT, "CAD", now);
        LedgerEntryEvent ledgerEntry = ledgerEntry(transactionId, BASE_AMOUNT, "CAD", now.plusMillis(500));

        return switch (scenario) {
            case MATCHED -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.PAYMENT_FIRST, 500,
                    payment, ledgerEntry, false
            );
            case AMOUNT_MISMATCH -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.PAYMENT_FIRST, 500,
                    payment,
                    ledgerEntry(transactionId, new BigDecimal("121.00"), "CAD", now.plusMillis(500)),
                    false
            );
            case CURRENCY_MISMATCH -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.PAYMENT_FIRST, 500,
                    payment,
                    ledgerEntry(transactionId, BASE_AMOUNT, "USD", now.plusMillis(500)),
                    false
            );
            case MISSING_LEDGER_ENTRY -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.PAYMENT_FIRST, 0,
                    payment, null, false
            );
            case MISSING_PAYMENT -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.LEDGER_FIRST, 0,
                    null, ledgerEntry, false
            );
            case DUPLICATE_PAYMENT -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.PAYMENT_FIRST, 500,
                    payment, ledgerEntry, true
            );
            case OUT_OF_ORDER_MATCH -> publishEvents(
                    transactionId, scenario.name(), now, EventOrder.LEDGER_FIRST, 500,
                    payment, ledgerEntry, false
            );
        };
    }

    public SubmissionResponse publish(TransactionRequest request) {
        validate(request);

        Instant now = clock.instant();
        String transactionId = normalizeTransactionId(request.transactionId());
        PaymentEvent payment = request.paymentAmount() == null ? null : payment(
                transactionId,
                request.paymentAmount(),
                request.paymentCurrency(),
                request.eventOrder() == EventOrder.PAYMENT_FIRST ? now : now.plusMillis(request.eventDelayMs())
        );
        LedgerEntryEvent ledgerEntry = request.ledgerAmount() == null ? null : ledgerEntry(
                transactionId,
                request.ledgerAmount(),
                request.ledgerCurrency(),
                request.eventOrder() == EventOrder.LEDGER_FIRST ? now : now.plusMillis(request.eventDelayMs())
        );

        return publishEvents(
                transactionId,
                "CUSTOM",
                now,
                request.eventOrder(),
                request.eventDelayMs(),
                payment,
                ledgerEntry,
                request.duplicatePayment()
        );
    }

    private SubmissionResponse publishEvents(
            String transactionId,
            String runType,
            Instant submittedAt,
            EventOrder eventOrder,
            long eventDelayMs,
            PaymentEvent payment,
            LedgerEntryEvent ledgerEntry,
            boolean duplicatePayment
    ) {
        List<EventEnvelope> orderedEvents = orderedEvents(
                eventOrder,
                payment,
                ledgerEntry,
                duplicatePayment
        );
        List<PublishedEvent> publishedEvents = new ArrayList<>();
        List<CompletableFuture<SendResult<String, Object>>> sends = new ArrayList<>();

        for (int index = 0; index < orderedEvents.size(); index++) {
            EventEnvelope envelope = orderedEvents.get(index);
            if (index > 0 && eventType(envelope.event()) != eventType(orderedEvents.get(index - 1).event())) {
                pause(eventDelayMs);
            }
            publish(envelope, index + 1, sends, publishedEvents);
        }

        awaitBrokerAcknowledgements(sends);
        return new SubmissionResponse(
                transactionId,
                runType,
                submittedAt,
                eventOrder,
                eventDelayMs,
                List.copyOf(publishedEvents)
        );
    }

    private List<EventEnvelope> orderedEvents(
            EventOrder eventOrder,
            PaymentEvent payment,
            LedgerEntryEvent ledgerEntry,
            boolean duplicatePayment
    ) {
        List<EventEnvelope> events = new ArrayList<>();
        if (eventOrder == EventOrder.LEDGER_FIRST) {
            addLedgerEntry(events, ledgerEntry);
            addPayment(events, payment, duplicatePayment);
        } else {
            addPayment(events, payment, duplicatePayment);
            addLedgerEntry(events, ledgerEntry);
        }
        return events;
    }

    private void addPayment(List<EventEnvelope> events, PaymentEvent payment, boolean duplicatePayment) {
        if (payment == null) {
            return;
        }
        events.add(new EventEnvelope(payment, false));
        if (duplicatePayment) {
            events.add(new EventEnvelope(payment, true));
        }
    }

    private void addLedgerEntry(List<EventEnvelope> events, LedgerEntryEvent ledgerEntry) {
        if (ledgerEntry != null) {
            events.add(new EventEnvelope(ledgerEntry, false));
        }
    }

    private void publish(
            EventEnvelope envelope,
            int sequence,
            List<CompletableFuture<SendResult<String, Object>>> sends,
            List<PublishedEvent> publishedEvents
    ) {
        Object event = envelope.event();
        if (event instanceof PaymentEvent payment) {
            sends.add(eventSender.send(topics.payments(), payment.transactionId(), payment));
            publishedEvents.add(new PublishedEvent(
                    sequence,
                    "PAYMENT",
                    topics.payments(),
                    payment.eventId(),
                    payment.transactionId(),
                    payment.amount(),
                    payment.currency(),
                    payment.processorReference(),
                    payment.occurredAt(),
                    envelope.duplicate()
            ));
        } else if (event instanceof LedgerEntryEvent ledgerEntry) {
            sends.add(eventSender.send(topics.ledgerEntries(), ledgerEntry.transactionId(), ledgerEntry));
            publishedEvents.add(new PublishedEvent(
                    sequence,
                    "LEDGER_ENTRY",
                    topics.ledgerEntries(),
                    ledgerEntry.eventId(),
                    ledgerEntry.transactionId(),
                    ledgerEntry.amount(),
                    ledgerEntry.currency(),
                    ledgerEntry.accountReference(),
                    ledgerEntry.occurredAt(),
                    envelope.duplicate()
            ));
        }
    }

    private Class<?> eventType(Object event) {
        return event.getClass();
    }

    private void pause(long eventDelayMs) {
        if (eventDelayMs == 0) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(eventDelayMs);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing the transaction", exception);
        }
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

    private void validate(TransactionRequest request) {
        if (request.paymentAmount() == null && request.ledgerAmount() == null) {
            throw new IllegalArgumentException("Publish at least one payment or ledger event");
        }
        if (request.paymentAmount() != null && isBlank(request.paymentCurrency())) {
            throw new IllegalArgumentException("paymentCurrency is required when publishing a payment");
        }
        if (request.ledgerAmount() != null && isBlank(request.ledgerCurrency())) {
            throw new IllegalArgumentException("ledgerCurrency is required when publishing a ledger entry");
        }
        if (request.duplicatePayment() && request.paymentAmount() == null) {
            throw new IllegalArgumentException("A duplicate payment requires a payment event");
        }
        if (request.eventDelayMs() < 0 || request.eventDelayMs() > 3000) {
            throw new IllegalArgumentException("eventDelayMs must be between 0 and 3000");
        }
    }

    private String normalizeTransactionId(String requestedId) {
        if (isBlank(requestedId)) {
            return "TX-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();
        }
        return requestedId.trim().toUpperCase();
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    private record EventEnvelope(Object event, boolean duplicate) {
    }
}
