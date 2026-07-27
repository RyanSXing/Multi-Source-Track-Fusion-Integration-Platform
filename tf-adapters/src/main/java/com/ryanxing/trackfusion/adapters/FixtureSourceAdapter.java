package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import java.util.List;
import java.util.Objects;
import reactor.core.publisher.Flux;

public final class FixtureSourceAdapter implements SourceAdapter {
    private final String sourceId;
    private final String sourceType;
    private final List<Detection> detections;

    public FixtureSourceAdapter(
            String sourceId, String sourceType, List<Detection> detections) {
        this.sourceId = requireText(sourceId, "sourceId");
        this.sourceType = requireText(sourceType, "sourceType");
        this.detections = List.copyOf(Objects.requireNonNull(detections, "detections"));
        if (this.detections.stream()
                .anyMatch(
                        detection ->
                                !sourceId.equals(detection.sourceId())
                                        || !sourceType.equals(detection.sourceType()))) {
            throw new IllegalArgumentException("fixture detections must match the adapter");
        }
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
        return Flux.fromIterable(detections);
    }

    private static String requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }
}
