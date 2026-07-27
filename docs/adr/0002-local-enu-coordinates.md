# ADR 0002: Fuse in a local ENU tangent plane

- Status: accepted
- Date: 2026-07-27

## Context

Latitude/longitude degrees are angular, have different physical scales, and cannot be used directly in a linear Kalman model.

## Decision

Convert WGS84 detections to metres in a configurable local East/North/Up tangent plane and convert snapshots back at the interface.

## Consequences

Covariance, velocity, gates, and source uncertainty share metre units. Accuracy is regional; global coverage would require geographic partitioning or an Earth-centred model.
