package com.ryanxing.trackfusion.common;

public record GeodeticPoint(double latDeg, double lonDeg, double altMeters) {
    public GeodeticPoint {
        if (!Double.isFinite(latDeg) || latDeg < -90 || latDeg > 90) {
            throw new IllegalArgumentException("invalid latitude");
        }
        if (!Double.isFinite(lonDeg) || lonDeg < -180 || lonDeg > 180) {
            throw new IllegalArgumentException("invalid longitude");
        }
        if (!Double.isFinite(altMeters)) {
            throw new IllegalArgumentException("invalid altitude");
        }
    }
}
