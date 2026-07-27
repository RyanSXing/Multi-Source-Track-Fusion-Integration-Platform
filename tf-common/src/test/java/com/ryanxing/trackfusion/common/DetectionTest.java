package com.ryanxing.trackfusion.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class DetectionTest {
    private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");

    @Test
    void keepsAValidatedImmutableSnapshot() {
        Map<String, String> attributes = new HashMap<>(Map.of("mmsi", "316001234"));

        Detection detection = detection(43.65, -79.38, 25.0, attributes);
        attributes.put("mmsi", "changed");

        assertThat(detection.attributes()).containsExactly(Map.entry("mmsi", "316001234"));
        assertThatThrownBy(() -> detection.attributes().put("x", "y"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsInvalidCoordinatesAndUncertainty() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> detection(90.1, -79.38, 25.0, Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> detection(43.65, -180.1, 25.0, Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> detection(43.65, -79.38, 0.0, Map.of()));
    }

    @Test
    void rejectsMissingIdentityTimeAndInvalidOptionalMeasurements() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Detection(
                        " ", "AIS", NOW, NOW, 43.65, -79.38, null, null, null, 25, Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Detection(
                        "ais-1", "AIS", null, NOW, 43.65, -79.38, null, null, null, 25, Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Detection(
                        "ais-1", "AIS", NOW, NOW, 43.65, -79.38, null, -1.0, null, 25, Map.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new Detection(
                        "ais-1", "AIS", NOW, NOW, 43.65, -79.38, null, null, 360.0, 25, Map.of()));
    }

    private static Detection detection(
            double latitude, double longitude, double sigma, Map<String, String> attributes) {
        return new Detection(
                "ais-1",
                "AIS",
                NOW,
                NOW.plusMillis(20),
                latitude,
                longitude,
                12.5,
                4.2,
                181.0,
                sigma,
                attributes);
    }
}
