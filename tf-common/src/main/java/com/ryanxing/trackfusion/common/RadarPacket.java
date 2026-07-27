package com.ryanxing.trackfusion.common;

import java.time.Instant;
import java.util.Map;

public record RadarPacket(
        String sourceId,
        String groundTruthId,
        Instant observedAt,
        double latDeg,
        double lonDeg,
        double altMeters,
        double speedMps,
        double headingDeg,
        double positionSigmaMeters) {

    public RadarPacket {
        if (groundTruthId == null || groundTruthId.isBlank()) {
            throw new IllegalArgumentException("groundTruthId is required");
        }
        new Detection(
                sourceId,
                "RADAR",
                observedAt,
                observedAt,
                latDeg,
                lonDeg,
                altMeters,
                speedMps,
                headingDeg,
                positionSigmaMeters,
                Map.of());
    }

    public Detection toDetection(Instant receivedAt) {
        return new Detection(
                sourceId,
                "RADAR",
                observedAt,
                receivedAt,
                latDeg,
                lonDeg,
                altMeters,
                speedMps,
                headingDeg,
                positionSigmaMeters,
                Map.of("groundTruthId", groundTruthId));
    }
}
