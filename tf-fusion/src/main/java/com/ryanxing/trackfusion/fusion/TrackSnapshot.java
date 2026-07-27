package com.ryanxing.trackfusion.fusion;

import com.ryanxing.trackfusion.common.Detection;
import java.time.Instant;
import java.util.List;

public record TrackSnapshot(
        long trackId,
        TrackStatus status,
        Instant stateAt,
        Instant lastObservedAt,
        double eastMeters,
        double northMeters,
        double eastVelocityMps,
        double northVelocityMps,
        int hitCount,
        int consecutiveMisses,
        List<Detection> contributors) {

    public TrackSnapshot {
        contributors = List.copyOf(contributors);
    }
}
