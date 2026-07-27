package com.ryanxing.trackfusion.service;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.fusion.TrackStatus;
import java.time.Instant;
import java.util.List;

public record TrackView(
        long trackId,
        TrackStatus status,
        Instant stateAt,
        Instant lastObservedAt,
        double latDeg,
        double lonDeg,
        Double altMeters,
        double eastVelocityMps,
        double northVelocityMps,
        int hitCount,
        int consecutiveMisses,
        List<Detection> contributors) {

    public TrackView {
        contributors = List.copyOf(contributors);
    }
}
