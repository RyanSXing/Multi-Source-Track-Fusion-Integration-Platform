package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ReplaySourceAdapter implements SourceAdapter {
    private final String sourceId;
    private final String sourceType;
    private final List<Detection> detections;
    private final double speedMultiplier;

    public ReplaySourceAdapter(
            String sourceId,
            String sourceType,
            List<Detection> detections,
            double speedMultiplier) {
        if (sourceId == null || sourceId.isBlank() || sourceType == null || sourceType.isBlank()) {
            throw new IllegalArgumentException("source identity is required");
        }
        if (!Double.isFinite(speedMultiplier) || speedMultiplier <= 0) {
            throw new IllegalArgumentException("speedMultiplier must be positive");
        }
        this.sourceId = sourceId;
        this.sourceType = sourceType;
        this.detections =
                Objects.requireNonNull(detections, "detections").stream()
                        .sorted(Comparator.comparing(Detection::observedAt))
                        .toList();
        if (this.detections.stream()
                .anyMatch(
                        detection ->
                                !sourceId.equals(detection.sourceId())
                                        || !sourceType.equals(detection.sourceType()))) {
            throw new IllegalArgumentException("replay detections must match the adapter");
        }
        this.speedMultiplier = speedMultiplier;
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    @Override
    public Flux<Detection> stream() {
        return Flux.defer(
                () -> {
                    AtomicReference<Instant> previous = new AtomicReference<>();
                    return Flux.fromIterable(detections)
                            .concatMap(
                                    detection -> {
                                        Instant prior = previous.getAndSet(detection.observedAt());
                                        if (prior == null) {
                                            return Mono.just(detection);
                                        }
                                        long delayNanos =
                                                (long)
                                                        (Duration.between(
                                                                                prior,
                                                                                detection
                                                                                        .observedAt())
                                                                        .toNanos()
                                                                / speedMultiplier);
                                        return Mono.delay(Duration.ofNanos(delayNanos))
                                                .thenReturn(detection);
                                    });
                });
    }
}
