package com.ryanxing.trackfusion.common;

public record EnuPoint(double eastMeters, double northMeters, double upMeters) {
    public EnuPoint {
        if (!Double.isFinite(eastMeters)
                || !Double.isFinite(northMeters)
                || !Double.isFinite(upMeters)) {
            throw new IllegalArgumentException("ENU coordinates must be finite");
        }
    }
}
