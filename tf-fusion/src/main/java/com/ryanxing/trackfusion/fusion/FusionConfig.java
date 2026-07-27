package com.ryanxing.trackfusion.fusion;

public record FusionConfig(
        int confirmationHits,
        int confirmationWindow,
        int dropAfterMisses,
        double associationGateSquared,
        double accelerationSigmaMps2) {

    public FusionConfig {
        if (confirmationHits < 1 || confirmationWindow < confirmationHits) {
            throw new IllegalArgumentException("invalid confirmation window");
        }
        if (dropAfterMisses < 1
                || !Double.isFinite(associationGateSquared)
                || associationGateSquared <= 0
                || !Double.isFinite(accelerationSigmaMps2)
                || accelerationSigmaMps2 <= 0) {
            throw new IllegalArgumentException("invalid fusion configuration");
        }
    }

    public static FusionConfig defaults() {
        return new FusionConfig(3, 5, 5, 9.21, 2);
    }
}
