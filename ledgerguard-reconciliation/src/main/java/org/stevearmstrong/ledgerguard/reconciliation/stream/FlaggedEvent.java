package org.stevearmstrong.ledgerguard.reconciliation.stream;

import org.stevearmstrong.ledgerguard.contracts.IdentifiedEvent;

record FlaggedEvent<T extends IdentifiedEvent>(T event, boolean duplicate) {
}
