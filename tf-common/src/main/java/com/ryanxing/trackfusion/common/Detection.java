package com.ryanxing.trackfusion.common;

import java.time.Instant;
import java.util.Map;

public record Detection(
        String sourceId,
        String sourceType,
        Instant observedAt,
        Instant receivedAt,
        double latDeg,
        double lonDeg,
        Double altMeters,
        Double speedMps,
        Double headingDeg,
        double positionSigmaMeters,
        Map<String, String> attributes) {

    public Detection {
        require(sourceId != null && !sourceId.isBlank(), "sourceId is required");
        require(sourceType != null && !sourceType.isBlank(), "sourceType is required");
        require(observedAt != null, "observedAt is required");
        require(receivedAt != null, "receivedAt is required");
        require(Double.isFinite(latDeg) && latDeg >= -90 && latDeg <= 90, "invalid latitude");
        require(Double.isFinite(lonDeg) && lonDeg >= -180 && lonDeg <= 180, "invalid longitude");
        require(altMeters == null || Double.isFinite(altMeters), "invalid altitude");
        require(speedMps == null || Double.isFinite(speedMps) && speedMps >= 0, "invalid speed");
        require(
                headingDeg == null
                        || Double.isFinite(headingDeg) && headingDeg >= 0 && headingDeg < 360,
                "invalid heading");
        require(
                Double.isFinite(positionSigmaMeters) && positionSigmaMeters > 0,
                "invalid position uncertainty");
        try {
            attributes = Map.copyOf(attributes == null ? Map.of() : attributes);
        } catch (NullPointerException invalidAttribute) {
            throw new IllegalArgumentException("attributes cannot contain nulls", invalidAttribute);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalArgumentException(message);
        }
    }
}
