package com.ryanxing.trackfusion.fusion;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.EnuPoint;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class FusionEngine {
    private static final Comparator<Detection> DETECTION_ORDER =
            Comparator.comparing(Detection::observedAt)
                    .thenComparing(Detection::sourceType)
                    .thenComparing(Detection::sourceId)
                    .thenComparingDouble(Detection::latDeg)
                    .thenComparingDouble(Detection::lonDeg);

    private final LocalTangentPlane plane;
    private final FusionConfig config;
    private final Map<Long, MutableTrack> activeTracks = new LinkedHashMap<>();
    private long nextTrackId = 1;
    private Instant lastTick;

    public FusionEngine(LocalTangentPlane plane) {
        this(plane, FusionConfig.defaults());
    }

    public FusionEngine(LocalTangentPlane plane, FusionConfig config) {
        this.plane = Objects.requireNonNull(plane, "plane");
        this.config = Objects.requireNonNull(config, "config");
    }

    public synchronized List<TrackSnapshot> update(Instant at, List<Detection> detections) {
        Objects.requireNonNull(at, "at");
        Objects.requireNonNull(detections, "detections");
        if (lastTick != null && at.isBefore(lastTick)) {
            throw new IllegalArgumentException("fusion updates must be chronological");
        }
        lastTick = at;

        List<Measurement> measurements =
                detections.stream()
                        .map(detection -> new Measurement(detection, toEnu(detection)))
                        .sorted(Comparator.comparing(Measurement::detection, DETECTION_ORDER))
                        .toList();
        List<MutableTrack> existing = new ArrayList<>(activeTracks.values());
        existing.forEach(track -> track.predict(at));

        double[][] costs = new double[existing.size()][measurements.size()];
        for (int track = 0; track < existing.size(); track++) {
            for (int measurement = 0; measurement < measurements.size(); measurement++) {
                Measurement candidate = measurements.get(measurement);
                costs[track][measurement] =
                        existing.get(track)
                                .filter
                                .mahalanobisSquared(
                                        candidate.position.eastMeters(),
                                        candidate.position.northMeters(),
                                        candidate.detection.positionSigmaMeters());
            }
        }

        int[] assignment =
                HungarianAssignment.solve(costs, config.associationGateSquared());
        boolean[] usedMeasurements = new boolean[measurements.size()];
        Set<Long> tracksHit = new HashSet<>();
        Map<Long, Set<String>> sourcesUsedThisTick = new HashMap<>();
        for (int track = 0; track < assignment.length; track++) {
            int measurement = assignment[track];
            if (measurement >= 0) {
                MutableTrack target = existing.get(track);
                Measurement match = measurements.get(measurement);
                target.hit(match);
                tracksHit.add(target.id);
                sourcesUsedThisTick
                        .computeIfAbsent(target.id, ignored -> new HashSet<>())
                        .add(match.detection.sourceType());
                usedMeasurements[measurement] = true;
            }
        }

        List<MutableTrack> births = new ArrayList<>();
        for (int index = 0; index < measurements.size(); index++) {
            if (usedMeasurements[index]) {
                continue;
            }
            Measurement measurement = measurements.get(index);
            MutableTrack target =
                    nearestAvailableTrack(
                            measurement, existing, births, sourcesUsedThisTick);
            if (target == null) {
                target = new MutableTrack(nextTrackId++, at, measurement);
                births.add(target);
            } else {
                target.hit(measurement);
            }
            tracksHit.add(target.id);
            sourcesUsedThisTick
                    .computeIfAbsent(target.id, ignored -> new HashSet<>())
                    .add(measurement.detection.sourceType());
        }

        for (MutableTrack track : existing) {
            if (!tracksHit.contains(track.id)) {
                track.miss();
            }
        }
        births.forEach(track -> activeTracks.put(track.id, track));

        List<TrackSnapshot> snapshots = snapshots(activeTracks.values());
        activeTracks.values().removeIf(track -> track.status == TrackStatus.DROPPED);
        return snapshots;
    }

    public synchronized List<TrackSnapshot> replay(List<Detection> detections) {
        Objects.requireNonNull(detections, "detections");
        activeTracks.clear();
        nextTrackId = 1;
        lastTick = null;

        List<Detection> ordered = detections.stream().sorted(DETECTION_ORDER).toList();
        for (int start = 0; start < ordered.size(); ) {
            Instant at = ordered.get(start).observedAt();
            int end = start + 1;
            while (end < ordered.size() && ordered.get(end).observedAt().equals(at)) {
                end++;
            }
            update(at, ordered.subList(start, end));
            start = end;
        }
        return tracks();
    }

    public synchronized List<TrackSnapshot> tracks() {
        return snapshots(activeTracks.values());
    }

    private EnuPoint toEnu(Detection detection) {
        return plane.toEnu(
                detection.latDeg(),
                detection.lonDeg(),
                detection.altMeters() == null ? 0 : detection.altMeters());
    }

    private MutableTrack nearestAvailableTrack(
            Measurement measurement,
            List<MutableTrack> existing,
            List<MutableTrack> births,
            Map<Long, Set<String>> sourcesUsedThisTick) {
        MutableTrack closest = null;
        double closestCost = config.associationGateSquared();
        for (MutableTrack track :
                java.util.stream.Stream.concat(existing.stream(), births.stream()).toList()) {
            if (sourcesUsedThisTick
                    .getOrDefault(track.id, Set.of())
                    .contains(measurement.detection.sourceType())) {
                continue;
            }
            double cost =
                    track.filter.mahalanobisSquared(
                            measurement.position.eastMeters(),
                            measurement.position.northMeters(),
                            measurement.detection.positionSigmaMeters());
            if (cost <= closestCost) {
                closest = track;
                closestCost = cost;
            }
        }
        return closest;
    }

    private static List<TrackSnapshot> snapshots(
            java.util.Collection<MutableTrack> tracks) {
        return tracks.stream()
                .sorted(Comparator.comparingLong(track -> track.id))
                .map(MutableTrack::snapshot)
                .toList();
    }

    private record Measurement(Detection detection, EnuPoint position) {}

    private final class MutableTrack {
        private final long id;
        private final KalmanFilter2D filter;
        private final Deque<Boolean> confirmationHistory = new ArrayDeque<>();
        private final Map<String, Detection> contributors = new HashMap<>();
        private TrackStatus status = TrackStatus.TENTATIVE;
        private Instant stateAt;
        private Instant lastObservedAt;
        private int hitCount = 1;
        private int consecutiveMisses;

        private MutableTrack(long id, Instant at, Measurement first) {
            this.id = id;
            this.stateAt = at;
            this.lastObservedAt = first.detection.observedAt();
            double speed =
                    first.detection.speedMps() == null ? 0 : first.detection.speedMps();
            double heading =
                    Math.toRadians(
                            first.detection.headingDeg() == null
                                    ? 0
                                    : first.detection.headingDeg());
            filter =
                    new KalmanFilter2D(
                            first.position.eastMeters(),
                            first.position.northMeters(),
                            speed * Math.sin(heading),
                            speed * Math.cos(heading),
                            first.detection.positionSigmaMeters(),
                            config.accelerationSigmaMps2());
            remember(first.detection);
            confirmationHistory.add(true);
            if (config.confirmationHits() == 1) {
                status = TrackStatus.CONFIRMED;
            }
        }

        private void predict(Instant at) {
            Duration elapsed = Duration.between(stateAt, at);
            filter.predict(elapsed.toNanos() / 1_000_000_000.0);
            stateAt = at;
        }

        private void hit(Measurement measurement) {
            filter.update(
                    measurement.position.eastMeters(),
                    measurement.position.northMeters(),
                    measurement.detection.positionSigmaMeters());
            lastObservedAt =
                    lastObservedAt.isAfter(measurement.detection.observedAt())
                            ? lastObservedAt
                            : measurement.detection.observedAt();
            hitCount++;
            consecutiveMisses = 0;
            remember(measurement.detection);
            recordConfirmation(true);
            if (status == TrackStatus.COASTING
                    || status == TrackStatus.TENTATIVE
                            && confirmationHistory.stream()
                                            .filter(Boolean::booleanValue)
                                            .count()
                                    >= config.confirmationHits()) {
                status = TrackStatus.CONFIRMED;
            }
        }

        private void miss() {
            consecutiveMisses++;
            recordConfirmation(false);
            if (status == TrackStatus.TENTATIVE) {
                if (confirmationHistory.size() == config.confirmationWindow()
                        && confirmationHistory.stream()
                                        .filter(Boolean::booleanValue)
                                        .count()
                                < config.confirmationHits()) {
                    status = TrackStatus.DROPPED;
                }
            } else if (consecutiveMisses >= config.dropAfterMisses()) {
                status = TrackStatus.DROPPED;
            } else {
                status = TrackStatus.COASTING;
            }
        }

        private void recordConfirmation(boolean hit) {
            confirmationHistory.addLast(hit);
            while (confirmationHistory.size() > config.confirmationWindow()) {
                confirmationHistory.removeFirst();
            }
        }

        private void remember(Detection detection) {
            contributors.put(detection.sourceType() + '\0' + detection.sourceId(), detection);
        }

        private TrackSnapshot snapshot() {
            List<Detection> orderedContributors =
                    contributors.values().stream()
                            .sorted(
                                    Comparator.comparing(Detection::sourceType)
                                            .thenComparing(Detection::sourceId))
                            .toList();
            return new TrackSnapshot(
                    id,
                    status,
                    stateAt,
                    lastObservedAt,
                    filter.eastMeters(),
                    filter.northMeters(),
                    filter.eastVelocityMps(),
                    filter.northVelocityMps(),
                    hitCount,
                    consecutiveMisses,
                    orderedContributors);
        }
    }
}
