package org.stevearmstrong.ledgerguard.reconciliation.stream;

import java.time.Duration;

import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.stevearmstrong.ledgerguard.contracts.IdentifiedEvent;

final class DeduplicationProcessor<T extends IdentifiedEvent>
        implements FixedKeyProcessor<String, T, FlaggedEvent<T>> {

    private final String stateStoreName;
    private final Duration retention;
    private FixedKeyProcessorContext<String, FlaggedEvent<T>> context;
    private WindowStore<String, Long> eventStore;

    DeduplicationProcessor(String stateStoreName, Duration retention) {
        this.stateStoreName = stateStoreName;
        this.retention = retention;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void init(FixedKeyProcessorContext<String, FlaggedEvent<T>> context) {
        this.context = context;
        this.eventStore = context.getStateStore(stateStoreName);
    }

    @Override
    public void process(FixedKeyRecord<String, T> record) {
        long timestamp = record.timestamp();
        String eventId = record.value().eventId().toString();
        boolean duplicate = wasSeen(eventId, timestamp);

        if (!duplicate) {
            eventStore.put(eventId, timestamp, timestamp);
        }

        context.forward(record.withValue(new FlaggedEvent<>(record.value(), duplicate)));
    }

    private boolean wasSeen(String eventId, long timestamp) {
        long from = Math.max(0, timestamp - retention.toMillis());
        try (WindowStoreIterator<Long> matches = eventStore.fetch(eventId, from, timestamp)) {
            return matches.hasNext();
        }
    }
}
