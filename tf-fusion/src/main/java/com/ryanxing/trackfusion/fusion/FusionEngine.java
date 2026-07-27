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
    private static final Comparator<Double> NULLABLE_DOUBLE =
            Comparator.nullsFirst(Double::compareTo);
    private static final Comparator<Detection> DETECTION_ORDER =
            Comparator.comparing(Detection::observedAt)
                    .thenComparing(Detection::sourceType)
                    .thenComparing(Detection::sourceId)
                    .thenComparingDouble(Detection::latDeg)
                    .thenComparingDouble(Detection::lonDeg)
                    .thenComparing(Detection::altMeters, NULLABLE_DOUBLE)
                    .thenComparing(Detection::speedMps, NULLABLE_DOUBLE)
                    .thenComparing(Detection::headingDeg, NULLABLE_DOUBLE)
                    .thenComparingDouble(Detection::positionSigmaMeters)
                    .thenComparing(Detection::receivedAt)
                    .thenComparing(Detection::attributes, FusionEngine::compareAttributes);

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
        if (lastTick != null && !at.isAfter(lastTick)) {
            throw new IllegalArgumentException("fusion ticks must be strictly increasing");
        }
        if (detections.stream()
                .anyMatch(
                        detection ->
                                detection == null
                                        || !detection.observedAt().equals(at))) {
            throw new IllegalArgumentException("all detections must match the fusion tick");
        }
        lastTick = at;

        List<Measurement> measurements =
                detections.stream()
                        .map(detection -> new Measurement(detection, toEnu(detection)))
                        .sorted(Comparator.comparing(Measurement::detection, DETECTION_ORDER))
                        .toList();
        List<MutableTrack> existing = new ArrayList<>(activeTracks.values());
        existing.forEach(track -> track.predict(at));

        Map<SourceKey, List<Measurement>> measurementsBySource = new LinkedHashMap<>();
        for (Measurement measurement : measurements) {
            measurementsBySource
                    .computeIfAbsent(
                            SourceKey.from(measurement.detection),
                            ignored -> new ArrayList<>())
                    .add(measurement);
        }

        List<MutableTrack> candidates = new ArrayList<>(existing);
        Set<Long> tracksHit = new HashSet<>();
        for (List<Measurement> sourceMeasurements : measurementsBySource.values()) {
            double[][] costs = new double[candidates.size()][sourceMeasurements.size()];
            for (int track = 0; track < candidates.size(); track++) {
                for (int measurement = 0;
                        measurement < sourceMeasurements.size();
                        measurement++) {
                    Measurement candidate = sourceMeasurements.get(measurement);
                    costs[track][measurement] =
                            candidates.get(track)
                                    .filter
                                    .mahalanobisSquared(
                                            candidate.position.eastMeters(),
                                            candidate.position.northMeters(),
                                            candidate.detection.positionSigmaMeters());
                }
            }

            int[] assignment =
                    HungarianAssignment.solve(costs, config.associationGateSquared());
            boolean[] usedMeasurements = new boolean[sourceMeasurements.size()];
            for (int track = 0; track < assignment.length; track++) {
                int measurement = assignment[track];
                if (measurement >= 0) {
                    MutableTrack target = candidates.get(track);
                    target.assimilate(sourceMeasurements.get(measurement));
                    tracksHit.add(target.id);
                    usedMeasurements[measurement] = true;
                }
            }
            for (int measurement = 0;
                    measurement < sourceMeasurements.size();
                    measurement++) {
                if (!usedMeasurements[measurement]) {
                    MutableTrack birth =
                            new MutableTrack(
                                    nextTrackId++, at, sourceMeasurements.get(measurement));
                    activeTracks.put(birth.id, birth);
                    candidates.add(birth);
                    tracksHit.add(birth.id);
                }
            }
        }

        for (MutableTrack track : existing) {
            if (tracksHit.contains(track.id)) {
                track.hitTick();
            } else {
                track.miss();
            }
        }

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

    private static List<TrackSnapshot> snapshots(
            java.util.Collection<MutableTrack> tracks) {
        return tracks.stream()
                .sorted(Comparator.comparingLong(track -> track.id))
                .map(MutableTrack::snapshot)
                .toList();
    }

    private static int compareAttributes(
            Map<String, String> left, Map<String, String> right) {
        List<Map.Entry<String, String>> leftEntries =
                left.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
        List<Map.Entry<String, String>> rightEntries =
                right.entrySet().stream().sorted(Map.Entry.comparingByKey()).toList();
        for (int index = 0; index < Math.min(leftEntries.size(), rightEntries.size()); index++) {
            int key =
                    leftEntries.get(index).getKey().compareTo(rightEntries.get(index).getKey());
            if (key != 0) {
                return key;
            }
            int value =
                    leftEntries.get(index).getValue().compareTo(rightEntries.get(index).getValue());
            if (value != 0) {
                return value;
            }
        }
        return Integer.compare(leftEntries.size(), rightEntries.size());
    }

    private record Measurement(Detection detection, EnuPoint position) {}

    private record SourceKey(String sourceType, String sourceId) {
        private static SourceKey from(Detection detection) {
            return new SourceKey(detection.sourceType(), detection.sourceId());
        }
    }

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
            boolean hasVelocity =
                    first.detection.speedMps() != null
                            && first.detection.headingDeg() != null;
            double speed = hasVelocity ? first.detection.speedMps() : 0;
            double heading =
                    hasVelocity ? Math.toRadians(first.detection.headingDeg()) : 0;
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

        private void assimilate(Measurement measurement) {
            filter.update(
                    measurement.position.eastMeters(),
                    measurement.position.northMeters(),
                    measurement.detection.positionSigmaMeters());
            lastObservedAt =
                    lastObservedAt.isAfter(measurement.detection.observedAt())
                            ? lastObservedAt
                            : measurement.detection.observedAt();
            remember(measurement.detection);
        }

        private void hitTick() {
            hitCount++;
            consecutiveMisses = 0;
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
