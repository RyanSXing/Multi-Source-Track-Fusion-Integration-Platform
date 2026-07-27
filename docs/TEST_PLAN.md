# Test Plan

## Objective

Verify schema validation, coordinate correctness, deterministic fusion, source resilience, durable delivery, failover fencing, public interfaces, operator-console behavior, and measurable performance. The acceptance line-coverage target is at least 85% in `tf-common` and `tf-fusion`.

## Test levels

| Level | Scope | Execution |
|---|---|---|
| Unit | records, transforms, radar codec, adapters, retry, Kalman filter, association, lifecycle, event-time batching, API/WebSocket projections | `./mvnw test` |
| Coverage gate | line coverage for framework-free core modules | `./mvnw -pl tf-common,tf-fusion -am verify` |
| Integration | real PostgreSQL persistence/fencing and full fixture adapter → Redpanda → inbox → fusion → history path | `./mvnw -pl tf-service -am -Dtest='*IntegrationTest' -Dsurefire.failIfNoSpecifiedTests=false test` |
| Frontend build | TypeScript and production bundle | `cd tf-console && npm ci && npm run build` |
| System | Docker Compose health, REST, WebSocket, metrics, source health, fused fixture output | `docker compose up --build -d` |
| Browser | desktop/mobile layout, map tiles and symbols, track selection, contributor evidence, overflow, accessibility tree, console errors | Playwright against `http://localhost:3000` |
| Performance | Kafka replay throughput, delivery ratio, WebSocket end-to-end latency, process CPU, duplicate-track reduction | `tools/load_harness.py` |

## Functional coverage

| Area | Cases |
|---|---|
| Common schema | required identity/timestamps, finite/ranged coordinates, immutable attributes |
| Coordinates | known ENU offsets, altitude, round trip |
| Radar codec | round trip, truncation, malformed lengths |
| Adapters | OpenSky and AIS parsing, weather cache, UDP decode, replay timing, radar noise/dropout, retry/repeat lifecycle |
| Fusion | weighted measurement, chi-square gating, deterministic ordering, global assignment, cross-source seeding, confirmation, coasting, dropping |
| Delivery | database staging before acknowledgement, offset deduplication, same-tick cross-poll grouping, lateness, exact retry membership |
| Failover | advisory lock ownership, standby rejection, forced backend termination, epoch takeover, stale commit rejection, permanent retirement |
| Interfaces | current/session/history/source health, initial WebSocket snapshot, ordered deltas, periodic recovery snapshot |

## Acceptance criteria and recorded result

Recorded on 2026-07-27:

| Criterion | Result |
|---|---|
| `tf-common` line coverage ≥85% | Pass — 87.73% (143/163) |
| `tf-fusion` line coverage ≥85% | Pass — 91.63% (383/418) |
| Unit and integration tests | Pass — no failures |
| Real Redpanda/PostgreSQL end to end | Pass — 3 fixture detections became 2 persisted tracks, including ADS-B + radar fusion |
| Forced leader database-session loss | Pass — standby epoch advanced, stale writer rejected, inbox committed once |
| Vue production build | Pass |
| Desktop browser | Pass — live basemap, both track symbols, detail evidence, no console errors |
| Mobile 390×844 | Pass — no horizontal overflow, live map and symbols, stacked source/detail panels |
| Stress delivery | Pass — 240/240 observations measured |

## Performance procedure

1. Start the Compose stack and wait for `/actuator/health`.
2. Create a local Python environment and install `tools/requirements.txt`.
3. Run the isolated baseline on a fresh service session.
4. Run the higher-rate replay with `--cycles 40 --multiplier 20`.
5. Preserve the generated JSON reports; do not copy aspirational values into documentation.

Current reports:

| Run | Throughput | Delivery | p50 / p95 / p99 | Average / max CPU | Duplicate reduction |
|---|---:|---:|---:|---:|---:|
| Stress | 100.62 msg/s | 100% | 3001.97 / 3545.18 / 3663.18 ms | 5.58 / 33.68% | 50% |
| Isolated baseline | 9.25 msg/s | 100% | 2746.79 / 3419.71 / 3419.71 ms | 2.65 / 5.47% | 50% |

Both runs passed complete-delivery and historical ground-truth integrity gates: no mixed, missing, or fragmented objects. The higher-rate run coalesced repeated same-source/object observations within each event-time tick.

## CI evidence

GitHub Actions is the authoritative public pipeline. `.gitlab-ci.yml` and `Jenkinsfile` contain equivalent stages, but their runs must not be claimed until a GitLab mirror and Jenkins agent have actually executed them. Integration runners require Docker access for Testcontainers.
