# ADR 0004: Couple a durable Kafka inbox with epoch-fenced leadership

- Status: accepted
- Date: 2026-07-27

## Context

Acknowledging Kafka before durable work loses data; committing after a new leader takes over can duplicate or corrupt track history. An advisory lock alone does not invalidate an already-running database transaction.

## Decision

Stage each Kafka offset in PostgreSQL, process it once by event-time tick, and commit history with its inbox outcome. The active instance holds a PostgreSQL advisory lock and every write transaction verifies a durable leadership epoch/session under row lock.

## Consequences

Redelivery is idempotent, failed ticks retain exact membership, takeover is serialized against in-flight writes, and stale leaders fail closed. There is one active fusion writer and a takeover begins a new session because in-memory covariance is not replicated.
