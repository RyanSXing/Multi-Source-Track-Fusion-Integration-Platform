# Sprint Record

## Sprint 1 — ingest and fusion foundation

Goal: establish a reviewable Java 21 core and prove heterogeneous normalization.

- Maven modules, Compose infrastructure, and schema validation
- WGS84 ↔ local ENU transforms
- Radar binary codec/simulator and replay path
- OpenSky, AISStream, Open-Meteo, and UDP adapters
- Constant-velocity Kalman filter, Hungarian association, and lifecycle
- Outcome: deterministic pure-Java fusion with fast unit tests

Review adjustment: source ordering and cross-source seeding were made deterministic before service integration.

## Sprint 2 — durable service and operator workflow

Goal: make fusion observable, recoverable, demonstrable, and release-gated.

- Kafka publication/consumption, durable inbox, event-time watermark, R2DBC history
- WebFlux REST/WebSocket interfaces and Prometheus metrics
- Advisory-lock leadership plus durable epoch fencing
- Vue/MapLibre operator console with contributor evidence
- Testcontainers end-to-end test, 85% JaCoCo gate, three CI definitions
- Load harness, measured reports, design review, test plan, and ADRs
- Outcome: locally verified Compose system and publication-ready branch

Review adjustments: preserved failed-tick membership across retries, retired stale leaders permanently, fixed the map container CSS precedence defect, and retained measured benchmark misses instead of converting targets into claims.
