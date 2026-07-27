package com.ryanxing.trackfusion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.EnuPoint;
import com.ryanxing.trackfusion.common.GeodeticPoint;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.fusion.FusionEngine;
import com.ryanxing.trackfusion.fusion.TrackSnapshot;
import com.ryanxing.trackfusion.fusion.TrackStatus;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public final class TrackService implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TrackService.class);
    private static final String FUSION_LOCK =
            "SELECT pg_try_advisory_lock(734622460129) AS acquired";
    private static final String LEADER_PING = "SELECT true AS acquired";
    private static final TypeReference<List<Detection>> DETECTIONS =
            new TypeReference<>() {};

    private final FusionEngine engine;
    private final LocalTangentPlane plane;
    private final TrackHistoryRepository history;
    private final ProcessedKafkaRecordRepository processedRecords;
    private final TransactionalOperator transactions;
    private final ObjectMapper json;
    private final ConnectionFactory connections;
    private final DatabaseClient database;
    private final String sessionId = UUID.randomUUID().toString();
    private final Map<Long, TrackView> current = new ConcurrentHashMap<>();
    private final Map<Instant, PendingTick> pending = new HashMap<>();
    private final Sinks.Many<TrackEvent> updates =
            Sinks.many().replay().latest();
    private final AtomicLong version = new AtomicLong();
    private final Timer latency;
    private final Counter persistenceErrors;
    private final Counter emissionFailures;
    private final Mono<Void> initialized;
    private Instant lastTick;
    private Connection leaderConnection;
    private long leadershipEpoch;
    private boolean retired;

    public TrackService(
            FusionEngine engine,
            LocalTangentPlane plane,
            TrackHistoryRepository history,
            ProcessedKafkaRecordRepository processedRecords,
            TransactionalOperator transactions,
            ObjectMapper json,
            MeterRegistry meters,
            ConnectionFactory connections) {
        this.engine = engine;
        this.plane = plane;
        this.history = history;
        this.processedRecords = processedRecords;
        this.transactions = transactions;
        this.json = json;
        this.connections = connections;
        database = connections == null ? null : DatabaseClient.create(connections);
        initialized =
                history.findMaxTrackId()
                        .defaultIfEmpty(0L)
                        .doOnNext(max -> engine.initializeNextTrackId(max + 1))
                        .then()
                        .retryWhen(
                                reactor.util.retry.Retry.backoff(
                                                Long.MAX_VALUE,
                                                Duration.ofSeconds(1))
                                        .maxBackoff(Duration.ofSeconds(30))
                                        .doBeforeRetry(
                                                retry ->
                                                        LOG.warn(
                                                                "Retrying track history initialization",
                                                                retry.failure())))
                        .cache();
        latency =
                Timer.builder("track_fusion_end_to_end_latency")
                        .publishPercentileHistogram()
                        .register(meters);
        persistenceErrors = meters.counter("track_fusion_persistence_errors_total");
        emissionFailures = meters.counter("track_fusion_websocket_emission_failures_total");
        Gauge.builder("track_fusion_active_tracks", current, Map::size).register(meters);
    }

    TrackService(
            FusionEngine engine,
            LocalTangentPlane plane,
            TrackHistoryRepository history,
            ProcessedKafkaRecordRepository processedRecords,
            TransactionalOperator transactions,
            ObjectMapper json,
            MeterRegistry meters) {
        this(
                engine,
                plane,
                history,
                processedRecords,
                transactions,
                json,
                meters,
                null);
    }

    public synchronized Mono<Boolean> claimLeadership() {
        if (retired) {
            return Mono.just(false);
        }
        if (connections == null) {
            return Mono.just(true);
        }
        if (leaderConnection != null) {
            Connection held = leaderConnection;
            return query(held, LEADER_PING)
                    .onErrorResume(
                            error -> {
                                LOG.warn("Lost the fusion leadership connection", error);
                                return retireLeadership().thenReturn(false);
                            });
        }
        return Mono.from(connections.create())
                .flatMap(
                        connection ->
                                query(connection, FUSION_LOCK)
                                        .flatMap(
                                                acquired -> {
                                                    if (acquired) {
                                                        return advanceEpoch(connection)
                                                                .map(
                                                                        epoch -> {
                                                                            leaderConnection =
                                                                                    connection;
                                                                            leadershipEpoch =
                                                                                    epoch;
                                                                            LOG.info(
                                                                                    "Acquired fusion leadership epoch {} for session {}",
                                                                                    epoch,
                                                                                    sessionId);
                                                                            return true;
                                                                        });
                                                    }
                                                    return close(connection).thenReturn(false);
                                                })
                                        .onErrorResume(
                                                error ->
                                                        close(connection)
                                                                .then(Mono.error(error))));
    }

    public Mono<StageResult> stage(List<KafkaDetection> records) {
        List<String> ids = records.stream().map(KafkaDetection::id).toList();
        return processedRecords
                .findAllById(ids)
                .map(ProcessedKafkaRecordEntity::id)
                .collectList()
                .map(HashSet::new)
                .flatMap(
                        existing -> {
                            List<ProcessedKafkaRecordEntity> fresh =
                                    records.stream()
                                            .filter(record -> !existing.contains(record.id()))
                                            .map(record -> entity(record, "PENDING"))
                                            .toList();
                            return fresh.isEmpty()
                                    ? Mono.just(new StageResult(existing))
                                    : transactions
                                            .transactional(processedRecords.saveAll(fresh).then())
                                            .thenReturn(new StageResult(existing));
                        });
    }

    public Flux<KafkaDetection> pendingRecords() {
        return processedRecords
                .findByOutcomeOrderByObservedAtAsc("PENDING")
                .map(this::kafkaDetection);
    }

    public Mono<ProcessResult> process(
            Instant tick, List<KafkaDetection> records) {
        List<String> ids = records.stream().map(KafkaDetection::id).toList();
        return initialized.then(
                processedRecords
                        .findAllById(ids)
                        .filter(row -> !"PENDING".equals(row.outcome()))
                        .map(ProcessedKafkaRecordEntity::id)
                        .collectList()
                        .map(HashSet::new)
                        .flatMap(existing -> persist(tick, records, existing)));
    }

    private Mono<ProcessResult> persist(
            Instant tick, List<KafkaDetection> records, Set<String> existing) {
        List<KafkaDetection> fresh =
                records.stream().filter(record -> !existing.contains(record.id())).toList();
        if (fresh.isEmpty()) {
            PendingTick committed;
            synchronized (this) {
                committed = pending.get(tick);
            }
            return committed == null
                    ? Mono.just(new ProcessResult(existing, Set.of()))
                    : Mono.fromCallable(() -> complete(committed, existing));
        }

        PendingTick work;
        synchronized (this) {
            work = pending.get(tick);
            if (work == null) {
                boolean late = lastTick != null && !tick.isAfter(lastTick);
                List<TrackView> tracks =
                        late
                                ? List.of()
                                : engine.updateAt(tick, fresh.stream()
                                                .map(KafkaDetection::detection)
                                                .toList())
                                        .stream()
                                        .map(this::view)
                                        .toList();
                work = new PendingTick(tick, fresh, tracks, late);
                pending.put(tick, work);
                if (!late) {
                    recordLatency(tick, fresh);
                }
            }
        }

        PendingTick pendingTick = work;
        Mono<Void> writes =
                fenceLeadership()
                        .thenMany(
                                history.saveAll(
                                pendingTick.tracks().stream()
                                        .map(this::entity)
                                        .toList()))
                        .then(
                                processedRecords
                                        .saveAll(
                                                pendingTick.records().stream()
                                                        .map(
                                                                record ->
                                                                        entity(
                                                                                record,
                                                                                pendingTick.late()
                                                                                        ? "LATE"
                                                                                        : "FUSED"))
                                                        .toList())
                                        .then());
        return transactions
                .transactional(writes)
                .then(Mono.fromCallable(() -> complete(pendingTick, existing)))
                .onErrorResume(
                        LeadershipLostException.class,
                        error ->
                                retireLeadership()
                                        .then(Mono.error(error)))
                .doOnError(
                        error -> {
                            persistenceErrors.increment();
                            LOG.error("Could not persist fusion tick {}", tick, error);
                        });
    }

    private synchronized ProcessResult complete(
            PendingTick work, Set<String> duplicates) {
        pending.remove(work.tick());
        Set<String> late = new HashSet<>();
        if (work.late()) {
            work.records().forEach(record -> late.add(record.id()));
        } else {
            lastTick = work.tick();
            work.tracks().forEach(
                    track -> {
                        if (track.status() == TrackStatus.DROPPED) {
                            current.remove(track.trackId());
                        } else {
                            current.put(track.trackId(), track);
                        }
                    });
            TrackEvent event =
                    new TrackEvent(
                            sessionId, version.incrementAndGet(), false, work.tracks());
            Sinks.EmitResult result = updates.tryEmitNext(event);
            if (result.isFailure() && result != Sinks.EmitResult.FAIL_ZERO_SUBSCRIBER) {
                emissionFailures.increment();
            }
        }
        return new ProcessResult(Set.copyOf(duplicates), Set.copyOf(late));
    }

    private void recordLatency(Instant tick, List<KafkaDetection> records) {
        records.stream()
                .map(KafkaDetection::detection)
                .forEach(
                        detection -> {
                            Duration elapsed = Duration.between(detection.receivedAt(), tick);
                            if (!elapsed.isNegative()) {
                                latency.record(elapsed);
                            }
                        });
    }

    public synchronized List<TrackView> currentTracks() {
        return current.values().stream()
                .sorted(Comparator.comparingLong(TrackView::trackId))
                .toList();
    }

    public Flux<TrackView> history(long trackId) {
        return history.findByTrackIdOrderByStateAtAsc(trackId).map(this::view);
    }

    public synchronized TrackEvent snapshot() {
        return new TrackEvent(sessionId, version.get(), true, currentTracks());
    }

    public Flux<TrackEvent> updates() {
        return updates.asFlux();
    }

    public String sessionId() {
        return sessionId;
    }

    private TrackView view(TrackSnapshot track) {
        GeodeticPoint point =
                plane.toGeodetic(
                        new EnuPoint(track.eastMeters(), track.northMeters(), 0));
        Double altitude =
                track.contributors().stream()
                        .map(Detection::altMeters)
                        .filter(java.util.Objects::nonNull)
                        .findFirst()
                        .orElse(null);
        return new TrackView(
                sessionId,
                track.trackId(),
                track.status(),
                track.stateAt(),
                track.lastObservedAt(),
                point.latDeg(),
                point.lonDeg(),
                altitude,
                track.eastVelocityMps(),
                track.northVelocityMps(),
                track.hitCount(),
                track.consecutiveMisses(),
                track.contributors());
    }

    private TrackHistoryEntity entity(TrackView track) {
        try {
            return new TrackHistoryEntity(
                    null,
                    track.sessionId(),
                    track.trackId(),
                    track.status().name(),
                    track.stateAt(),
                    track.lastObservedAt(),
                    track.latDeg(),
                    track.lonDeg(),
                    track.altMeters(),
                    track.eastVelocityMps(),
                    track.northVelocityMps(),
                    track.hitCount(),
                    track.consecutiveMisses(),
                    json.writeValueAsString(track.contributors()));
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize track contributors", error);
        }
    }

    private TrackView view(TrackHistoryEntity row) {
        try {
            return new TrackView(
                    row.sessionId(),
                    row.trackId(),
                    TrackStatus.valueOf(row.status()),
                    row.stateAt(),
                    row.lastObservedAt(),
                    row.latDeg(),
                    row.lonDeg(),
                    row.altMeters(),
                    row.eastVelocityMps(),
                    row.northVelocityMps(),
                    row.hitCount(),
                    row.consecutiveMisses(),
                    json.readValue(row.contributorsJson(), DETECTIONS));
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not read track contributors", error);
        }
    }

    private ProcessedKafkaRecordEntity entity(
            KafkaDetection record, String outcome) {
        try {
            return new ProcessedKafkaRecordEntity(
                    record.id(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.detection().observedAt(),
                    outcome,
                    json.writeValueAsString(record.detection()),
                    "PENDING".equals(outcome));
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize Kafka detection", error);
        }
    }

    private KafkaDetection kafkaDetection(ProcessedKafkaRecordEntity row) {
        try {
            return new KafkaDetection(
                    row.topic(),
                    row.partition(),
                    row.offset(),
                    json.readValue(row.detectionJson(), Detection.class));
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not read staged Kafka detection", error);
        }
    }

    private static Mono<Boolean> query(Connection connection, String sql) {
        return Mono.from(connection.createStatement(sql).execute())
                .flatMap(
                        result ->
                                Mono.from(
                                        result.map(
                                                (row, metadata) ->
                                                        row.get(
                                                                "acquired",
                                                                Boolean.class))))
                .map(Boolean.TRUE::equals);
    }

    private Mono<Long> advanceEpoch(Connection connection) {
        return Mono.from(
                        connection
                                .createStatement(
                                        """
                                        UPDATE fusion_leadership
                                        SET epoch = epoch + 1, session_id = $1
                                        WHERE name = 'fusion'
                                        RETURNING epoch
                                        """)
                                .bind(0, sessionId)
                                .execute())
                .flatMap(
                        result ->
                                Mono.from(
                                        result.map(
                                                (row, metadata) ->
                                                        row.get("epoch", Long.class))));
    }

    private Mono<Void> fenceLeadership() {
        if (database == null) {
            return Mono.empty();
        }
        return database.sql(
                        """
                        UPDATE fusion_leadership
                        SET session_id = session_id
                        WHERE name = 'fusion'
                          AND epoch = :epoch
                          AND session_id = :sessionId
                        """)
                .bind("epoch", leadershipEpoch)
                .bind("sessionId", sessionId)
                .fetch()
                .rowsUpdated()
                .flatMap(
                        rows ->
                                rows == 1
                                        ? Mono.empty()
                                        : Mono.error(
                                                new LeadershipLostException()));
    }

    private synchronized Mono<Void> retireLeadership() {
        retired = true;
        Connection held = leaderConnection;
        leaderConnection = null;
        return held == null
                ? Mono.empty()
                : Mono.from(held.close()).onErrorComplete();
    }

    private synchronized Mono<Void> close(Connection connection) {
        if (leaderConnection == connection) {
            leaderConnection = null;
        }
        return Mono.from(connection.close());
    }

    @Override
    public synchronized void close() {
        retired = true;
        if (leaderConnection != null) {
            close(leaderConnection).block();
        }
    }

    public record KafkaDetection(
            String topic, int partition, long offset, Detection detection) {
        public String id() {
            return topic + ':' + partition + ':' + offset;
        }
    }

    public record StageResult(Set<String> duplicateIds) {
        public StageResult {
            duplicateIds = Set.copyOf(duplicateIds);
        }
    }

    public record ProcessResult(Set<String> duplicateIds, Set<String> lateIds) {}

    public record TrackEvent(
            String sessionId, long version, boolean snapshot, List<TrackView> tracks) {
        public TrackEvent {
            tracks = List.copyOf(tracks);
        }
    }

    private record PendingTick(
            Instant tick,
            List<KafkaDetection> records,
            List<TrackView> tracks,
            boolean late) {}

    private static final class LeadershipLostException
            extends IllegalStateException {
        private LeadershipLostException() {
            super("Fusion leadership changed before commit");
        }
    }
}
