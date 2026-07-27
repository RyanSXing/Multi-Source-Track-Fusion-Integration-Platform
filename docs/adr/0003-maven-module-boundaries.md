# ADR 0003: Separate schema, adapters, fusion, and service Maven modules

- Status: accepted
- Date: 2026-07-27

## Context

Fusion math must remain fast to test and independent from Spring, Kafka, and database lifecycle concerns.

## Decision

Use `tf-common`, `tf-adapters`, `tf-fusion`, and `tf-service` modules under one Maven reactor. `tf-fusion` depends only on `tf-common`.

## Consequences

Dependency direction is explicit, core coverage gates are meaningful, and service packaging remains simple. The small project has more POMs, but no module exists without a distinct dependency boundary.
