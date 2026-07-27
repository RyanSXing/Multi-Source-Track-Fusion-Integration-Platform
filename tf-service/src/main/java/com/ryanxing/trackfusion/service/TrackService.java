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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

public final class TrackService {
    private static final Logger LOG = LoggerFactory.getLogger(TrackService.class);
    private static final TypeReference<List<Detection>> DETECTIONS =
            new TypeReference<>() {};

    private final FusionEngine engine;
    private final LocalTangentPlane plane;
    private final TrackHistoryRepository repository;
    private final ObjectMapper json;
    private final Map<Long, TrackView> current = new ConcurrentHashMap<>();
    private final Sinks.Many<TrackView> updates = Sinks.many().multicast().directBestEffort();
    private final Timer latency;
    private final Counter persistenceErrors;

    public TrackService(
            FusionEngine engine,
            LocalTangentPlane plane,
            TrackHistoryRepository repository,
            ObjectMapper json,
            MeterRegistry meters) {
        this.engine = engine;
        this.plane = plane;
        this.repository = repository;
        this.json = json;
        latency =
                Timer.builder("track_fusion_end_to_end_latency")
                        .publishPercentileHistogram()
                        .register(meters);
        persistenceErrors = meters.counter("track_fusion_persistence_errors_total");
        Gauge.builder("track_fusion_active_tracks", current, Map::size).register(meters);
    }

    public Mono<List<TrackView>> process(Instant tick, List<Detection> detections) {
        return Mono.fromCallable(
                        () -> {
                            List<TrackView> tracks =
                                    engine.updateAt(tick, detections).stream()
                                            .map(this::view)
                                            .toList();
                            detections.forEach(
                                    detection -> {
                                        Duration elapsed =
                                                Duration.between(detection.receivedAt(), tick);
                                        if (!elapsed.isNegative()) {
                                            latency.record(elapsed);
                                        }
                                    });
                            return tracks;
                        })
                .flatMap(
                        tracks ->
                                repository.saveAll(tracks.stream().map(this::entity).toList())
                                        .then()
                                        // ponytail: history is best-effort; add an outbox if
                                        // lossless database-outage recovery becomes required.
                                        .onErrorResume(
                                                error -> {
                                                    persistenceErrors.increment();
                                                    LOG.error("Could not persist track history", error);
                                                    return Mono.empty();
                                                })
                                        .thenReturn(tracks))
                .doOnNext(
                        tracks ->
                                tracks.forEach(
                                        track -> {
                                            if (track.status() == TrackStatus.DROPPED) {
                                                current.remove(track.trackId());
                                            } else {
                                                current.put(track.trackId(), track);
                                            }
                                            updates.tryEmitNext(track);
                                        }));
    }

    public List<TrackView> currentTracks() {
        return current.values().stream()
                .sorted(Comparator.comparingLong(TrackView::trackId))
                .toList();
    }

    public Flux<TrackView> history(long trackId) {
        return repository.findByTrackIdOrderByStateAtAsc(trackId).map(this::view);
    }

    public Flux<TrackView> updates() {
        return updates.asFlux();
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
}
