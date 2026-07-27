package com.ryanxing.trackfusion.fusion;

final class KalmanFilter2D {
    private final double[] state = new double[4];
    private final double[][] covariance = new double[4][4];
    private final double accelerationVariance;

    KalmanFilter2D(
            double eastMeters,
            double northMeters,
            double eastVelocityMps,
            double northVelocityMps,
            double positionSigmaMeters,
            double accelerationSigmaMps2) {
        if (!finite(eastMeters, northMeters, eastVelocityMps, northVelocityMps)
                || !Double.isFinite(positionSigmaMeters)
                || positionSigmaMeters <= 0
                || !Double.isFinite(accelerationSigmaMps2)
                || accelerationSigmaMps2 <= 0) {
            throw new IllegalArgumentException("invalid Kalman filter state");
        }
        state[0] = eastMeters;
        state[1] = northMeters;
        state[2] = eastVelocityMps;
        state[3] = northVelocityMps;
        covariance[0][0] = positionSigmaMeters * positionSigmaMeters;
        covariance[1][1] = positionSigmaMeters * positionSigmaMeters;
        covariance[2][2] = 400;
        covariance[3][3] = 400;
        accelerationVariance = accelerationSigmaMps2 * accelerationSigmaMps2;
    }

    void predict(double elapsedSeconds) {
        if (!Double.isFinite(elapsedSeconds) || elapsedSeconds < 0) {
            throw new IllegalArgumentException("elapsedSeconds must be finite and non-negative");
        }
        if (elapsedSeconds == 0) {
            return;
        }
        state[0] += state[2] * elapsedSeconds;
        state[1] += state[3] * elapsedSeconds;

        double[][] transition = {
            {1, 0, elapsedSeconds, 0},
            {0, 1, 0, elapsedSeconds},
            {0, 0, 1, 0},
            {0, 0, 0, 1}
        };
        double[][] predicted = new double[4][4];
        for (int row = 0; row < 4; row++) {
            for (int column = 0; column < 4; column++) {
                for (int i = 0; i < 4; i++) {
                    for (int j = 0; j < 4; j++) {
                        predicted[row][column] +=
                                transition[row][i]
                                        * covariance[i][j]
                                        * transition[column][j];
                    }
                }
            }
        }
        double dt2 = elapsedSeconds * elapsedSeconds;
        double dt3 = dt2 * elapsedSeconds;
        double dt4 = dt2 * dt2;
        predicted[0][0] += accelerationVariance * dt4 / 4;
        predicted[0][2] += accelerationVariance * dt3 / 2;
        predicted[1][1] += accelerationVariance * dt4 / 4;
        predicted[1][3] += accelerationVariance * dt3 / 2;
        predicted[2][0] += accelerationVariance * dt3 / 2;
        predicted[2][2] += accelerationVariance * dt2;
        predicted[3][1] += accelerationVariance * dt3 / 2;
        predicted[3][3] += accelerationVariance * dt2;
        copy(predicted, covariance);
    }

    void update(double eastMeters, double northMeters, double positionSigmaMeters) {
        double[] inverse = innovationInverse(positionSigmaMeters);
        double residualEast = eastMeters - state[0];
        double residualNorth = northMeters - state[1];
        double[][] gain = new double[4][2];
        for (int row = 0; row < 4; row++) {
            gain[row][0] =
                    covariance[row][0] * inverse[0] + covariance[row][1] * inverse[2];
            gain[row][1] =
                    covariance[row][0] * inverse[1] + covariance[row][1] * inverse[3];
            state[row] += gain[row][0] * residualEast + gain[row][1] * residualNorth;
        }

        double[][] updated = new double[4][4];
        for (int row = 0; row < 4; row++) {
            for (int column = 0; column < 4; column++) {
                updated[row][column] =
                        covariance[row][column]
                                - gain[row][0] * covariance[0][column]
                                - gain[row][1] * covariance[1][column];
            }
        }
        for (int row = 0; row < 4; row++) {
            for (int column = row; column < 4; column++) {
                double symmetric = (updated[row][column] + updated[column][row]) / 2;
                covariance[row][column] = symmetric;
                covariance[column][row] = symmetric;
            }
            covariance[row][row] = Math.max(covariance[row][row], 1e-9);
        }
    }

    double mahalanobisSquared(
            double eastMeters, double northMeters, double positionSigmaMeters) {
        double[] inverse = innovationInverse(positionSigmaMeters);
        double east = eastMeters - state[0];
        double north = northMeters - state[1];
        return east * (inverse[0] * east + inverse[1] * north)
                + north * (inverse[2] * east + inverse[3] * north);
    }

    double eastMeters() {
        return state[0];
    }

    double northMeters() {
        return state[1];
    }

    double eastVelocityMps() {
        return state[2];
    }

    double northVelocityMps() {
        return state[3];
    }

    private double[] innovationInverse(double positionSigmaMeters) {
        if (!Double.isFinite(positionSigmaMeters) || positionSigmaMeters <= 0) {
            throw new IllegalArgumentException("positionSigmaMeters must be positive");
        }
        double variance = positionSigmaMeters * positionSigmaMeters;
        double a = covariance[0][0] + variance;
        double b = covariance[0][1];
        double c = covariance[1][0];
        double d = covariance[1][1] + variance;
        double determinant = a * d - b * c;
        if (!(determinant > 0) || !Double.isFinite(determinant)) {
            throw new IllegalStateException("singular innovation covariance");
        }
        return new double[] {d / determinant, -b / determinant, -c / determinant, a / determinant};
    }

    private static boolean finite(double... values) {
        for (double value : values) {
            if (!Double.isFinite(value)) {
                return false;
            }
        }
        return true;
    }

    private static void copy(double[][] source, double[][] target) {
        for (int row = 0; row < source.length; row++) {
            System.arraycopy(source[row], 0, target[row], 0, source[row].length);
        }
    }
}
