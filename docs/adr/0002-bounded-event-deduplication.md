# ADR 0002: Bound event-ID deduplication state

## Status

Accepted

## Context

At-least-once delivery can produce the same event more than once. A permanent set of every observed event ID would prevent duplicates, but its storage would grow without a bound.

## Decision

Payment and ledger event IDs are stored in separate persistent Kafka Streams window stores with a configurable 24-hour retention period.

Repeated IDs inside that retention window are routed as `DUPLICATE_PAYMENT` or `DUPLICATE_LEDGER_ENTRY`. Unique records continue to reconciliation.

## Consequences

State size is bounded and automatically backed by Kafka changelog topics. A duplicate arriving after the retention period is treated as a new event, so the retention must be chosen to exceed the maximum expected upstream retry horizon.
