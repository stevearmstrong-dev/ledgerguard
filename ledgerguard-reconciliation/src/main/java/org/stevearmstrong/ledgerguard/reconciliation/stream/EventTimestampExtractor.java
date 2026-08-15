package org.stevearmstrong.ledgerguard.reconciliation.stream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.stevearmstrong.ledgerguard.contracts.IdentifiedEvent;

final class EventTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() instanceof IdentifiedEvent event) {
            return event.occurredAt().toEpochMilli();
        }
        return record.timestamp() >= 0 ? record.timestamp() : partitionTime;
    }
}
