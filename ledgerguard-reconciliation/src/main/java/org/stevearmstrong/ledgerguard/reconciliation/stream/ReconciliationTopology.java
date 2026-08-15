package org.stevearmstrong.ledgerguard.reconciliation.stream;

import java.time.Duration;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.Stores;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JacksonJsonSerde;
import org.stevearmstrong.ledgerguard.contracts.LedgerEntryEvent;
import org.stevearmstrong.ledgerguard.contracts.PaymentEvent;
import org.stevearmstrong.ledgerguard.contracts.ReconciliationResult;
import org.stevearmstrong.ledgerguard.reconciliation.config.LedgerGuardTopics;
import org.stevearmstrong.ledgerguard.reconciliation.config.ReconciliationProperties;
import org.stevearmstrong.ledgerguard.reconciliation.domain.ReconciliationService;

@Configuration(proxyBeanMethods = false)
@EnableKafkaStreams
@EnableConfigurationProperties({LedgerGuardTopics.class, ReconciliationProperties.class})
public class ReconciliationTopology {

    static final String PAYMENT_EVENT_STORE = "payment-event-ids";
    static final String LEDGER_EVENT_STORE = "ledger-event-ids";

    @Bean
    KStream<String, ReconciliationResult> reconciliationStream(
            StreamsBuilder builder,
            LedgerGuardTopics topics,
            ReconciliationProperties properties,
            ReconciliationService reconciliationService,
            ReconciliationMetrics metrics
    ) {
        JacksonJsonSerde<PaymentEvent> paymentSerde = new JacksonJsonSerde<>(PaymentEvent.class);
        JacksonJsonSerde<LedgerEntryEvent> ledgerSerde = new JacksonJsonSerde<>(LedgerEntryEvent.class);
        JacksonJsonSerde<ReconciliationResult> resultSerde = new JacksonJsonSerde<>(ReconciliationResult.class);

        addDeduplicationStore(builder, PAYMENT_EVENT_STORE, properties.deduplicationRetention());
        addDeduplicationStore(builder, LEDGER_EVENT_STORE, properties.deduplicationRetention());

        KStream<String, PaymentEvent> payments = builder
                .stream(
                        topics.payments(),
                        Consumed.with(Serdes.String(), paymentSerde)
                                .withTimestampExtractor(new EventTimestampExtractor())
                )
                .selectKey(
                        (ignored, event) -> event.transactionId(),
                        Named.as("key-payments-by-transaction")
                );

        KStream<String, LedgerEntryEvent> ledgerEntries = builder
                .stream(
                        topics.ledgerEntries(),
                        Consumed.with(Serdes.String(), ledgerSerde)
                                .withTimestampExtractor(new EventTimestampExtractor())
                )
                .selectKey(
                        (ignored, event) -> event.transactionId(),
                        Named.as("key-ledger-entries-by-transaction")
                );

        KStream<String, FlaggedEvent<PaymentEvent>> flaggedPayments = payments.processValues(
                () -> new DeduplicationProcessor<>(PAYMENT_EVENT_STORE, properties.deduplicationRetention()),
                Named.as("deduplicate-payments"),
                PAYMENT_EVENT_STORE
        );
        KStream<String, FlaggedEvent<LedgerEntryEvent>> flaggedLedgerEntries = ledgerEntries.processValues(
                () -> new DeduplicationProcessor<>(LEDGER_EVENT_STORE, properties.deduplicationRetention()),
                Named.as("deduplicate-ledger-entries"),
                LEDGER_EVENT_STORE
        );

        KStream<String, ReconciliationResult> duplicatePayments = flaggedPayments
                .filter((key, flagged) -> flagged.duplicate(), Named.as("select-duplicate-payments"))
                .mapValues(
                        (key, flagged) -> reconciliationService.duplicatePayment(flagged.event()),
                        Named.as("classify-duplicate-payments")
                );

        KStream<String, ReconciliationResult> duplicateLedgerEntries = flaggedLedgerEntries
                .filter((key, flagged) -> flagged.duplicate(), Named.as("select-duplicate-ledger-entries"))
                .mapValues(
                        (key, flagged) -> reconciliationService.duplicateLedgerEntry(flagged.event()),
                        Named.as("classify-duplicate-ledger-entries")
                );

        KStream<String, PaymentEvent> uniquePayments = flaggedPayments
                .filterNot((key, flagged) -> flagged.duplicate(), Named.as("select-unique-payments"))
                .mapValues(FlaggedEvent::event, Named.as("unwrap-unique-payments"));

        KStream<String, LedgerEntryEvent> uniqueLedgerEntries = flaggedLedgerEntries
                .filterNot((key, flagged) -> flagged.duplicate(), Named.as("select-unique-ledger-entries"))
                .mapValues(FlaggedEvent::event, Named.as("unwrap-unique-ledger-entries"));

        JoinWindows windows = JoinWindows.ofTimeDifferenceAndGrace(
                properties.joinWindow(),
                properties.gracePeriod()
        );

        KStream<String, ReconciliationResult> joinedResults = uniquePayments.outerJoin(
                uniqueLedgerEntries,
                reconciliationService::reconcile,
                windows,
                StreamJoined.with(Serdes.String(), paymentSerde, ledgerSerde)
                        .withName("reconcile-transactions")
                        .withStoreName("transaction-reconciliation-window")
        );

        KStream<String, ReconciliationResult> results = joinedResults
                .merge(duplicatePayments, Named.as("merge-payment-duplicates"))
                .merge(duplicateLedgerEntries, Named.as("merge-ledger-duplicates"));

        results.peek((key, result) -> metrics.record(result), Named.as("record-reconciliation-metrics"));
        results.to(
                topics.reconciliations(),
                Produced.with(Serdes.String(), resultSerde).withName("publish-reconciliations")
        );
        results.filter(
                        (key, result) -> result.requiresReview(),
                        Named.as("select-review-required-results")
                )
                .to(
                        topics.exceptions(),
                        Produced.with(Serdes.String(), resultSerde).withName("publish-review-required-results")
                );

        return results;
    }

    private void addDeduplicationStore(StreamsBuilder builder, String name, Duration retention) {
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(name, retention, retention, false),
                Serdes.String(),
                Serdes.Long()
        ));
    }
}
