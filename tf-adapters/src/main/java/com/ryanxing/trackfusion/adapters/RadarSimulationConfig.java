package com.ryanxing.trackfusion.adapters;

import java.time.Duration;

public record RadarSimulationConfig(
        String sourceId,
        double positionNoiseMeters,
        double dropoutRate,
        Duration latency,
        double falseTargetRate) {

    public RadarSimulationConfig {
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException("sourceId is required");
        }
        if (!Double.isFinite(positionNoiseMeters)
                || positionNoiseMeters < 0
                || !probability(dropoutRate)
                || latency == null
                || latency.isNegative()
                || !probability(falseTargetRate)) {
            throw new IllegalArgumentException("invalid radar simulation configuration");
        }
    }

    private static boolean probability(double value) {
        return Double.isFinite(value) && value >= 0 && value <= 1;
    }
}
