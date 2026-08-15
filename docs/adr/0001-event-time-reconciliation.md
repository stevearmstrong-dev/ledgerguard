# ADR 0001: Reconcile with event-time stream-stream joins

## Status

Accepted

## Context

Payment and ledger records are produced independently. Network delays, retries, and upstream processing mean records can arrive out of order. Processing them strictly in arrival order would incorrectly classify legitimate transactions as missing.

## Decision

LedgerGuard uses the timestamp embedded in each event and a Kafka Streams outer join keyed by `transactionId`.

- The join accepts records within a configurable 10-second time difference.
- A 3-second grace period allows moderately late records to participate.
- An outer join preserves unmatched records so missing payments and ledger entries become explicit reconciliation outcomes.
- The Kafka Streams processing guarantee is set to `exactly_once_v2`.

## Consequences

The reconciliation decision is based on business event time instead of host processing time. Missing-record results are delayed until the window closes, which is an intentional correctness tradeoff. Increasing the window improves tolerance for late records but also increases state-store size and time-to-decision.
