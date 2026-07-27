# ADR 0001: Use Kafka-compatible Redpanda as the ingest boundary

- Status: accepted
- Date: 2026-07-27

## Context

External feeds fail and recover independently. Fusion needs a common replayable stream and must not run inside adapter transport callbacks.

## Decision

Publish normalized `Detection` values to one Kafka topic, using Redpanda locally for a smaller single-node Compose footprint.

## Consequences

Adapters and fusion scale/fail independently, and real broker behavior is integration-tested. The system gains operational broker state and cannot provide useful fusion until Kafka is available; publishers retry with bounded exponential delay.
