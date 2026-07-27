package com.ryanxing.trackfusion.fusion;

import static org.assertj.core.api.Assertions.assertThat;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.EnuPoint;
import com.ryanxing.trackfusion.common.GeodeticPoint;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FusionEngineTest {
    private static final LocalTangentPlane PLANE = new LocalTangentPlane(0, 0, 0);
    private static final Instant START = Instant.parse("2026-01-02T03:04:05Z");

    @Test
    void weightsMeasurementsByTheirReportedUncertainty() {
        KalmanFilter2D precise = new KalmanFilter2D(0, 0, 0, 0, 10, 1);
        KalmanFilter2D noisy = new KalmanFilter2D(0, 0, 0, 0, 10, 1);

        precise.update(100, 0, 1);
        noisy.update(100, 0, 1_000);

        assertThat(precise.eastMeters()).isGreaterThan(99);
        assertThat(noisy.eastMeters()).isLessThan(1);
    }

    @Test
    void findsTheGlobalMinimumAssignment() {
        int[] assignment =
                HungarianAssignment.solve(
                        new double[][] {
                            {1, 2},
                            {2, 100}
                        },
                        200);

        assertThat(assignment).containsExactly(1, 0);
    }

    @Test
    void appliesConfirmationCoastingAndDropLifecycle() {
        FusionEngine engine =
                new FusionEngine(PLANE, new FusionConfig(2, 3, 3, 9.21, 1));

        assertThat(engine.update(START, List.of(detection("adsb-1", "ADSB", START, 0))))
                .singleElement()
                .extracting(TrackSnapshot::status)
                .isEqualTo(TrackStatus.TENTATIVE);
        assertThat(
                        engine.update(
                                START.plusSeconds(1),
                                List.of(
                                        detection(
                                                "adsb-1",
                                                "ADSB",
                                                START.plusSeconds(1),
                                                0))))
                .singleElement()
                .extracting(TrackSnapshot::status)
                .isEqualTo(TrackStatus.CONFIRMED);
        assertThat(engine.update(START.plusSeconds(2), List.of()))
                .singleElement()
                .extracting(TrackSnapshot::status)
                .isEqualTo(TrackStatus.COASTING);
        assertThat(
                        engine.update(
                                START.plusSeconds(3),
                                List.of(
                                        detection(
                                                "radar-1",
                                                "RADAR",
                                                START.plusSeconds(3),
                                                0))))
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.status()).isEqualTo(TrackStatus.CONFIRMED);
                            assertThat(track.contributors())
                                    .extracting(Detection::sourceType)
                                    .containsExactlyInAnyOrder("ADSB", "RADAR");
                        });

        engine.update(START.plusSeconds(4), List.of());
        engine.update(START.plusSeconds(5), List.of());
        assertThat(engine.update(START.plusSeconds(6), List.of()))
                .singleElement()
                .extracting(TrackSnapshot::status)
                .isEqualTo(TrackStatus.DROPPED);
        assertThat(engine.tracks()).isEmpty();
    }

    @Test
    void replayIsDeterministicRegardlessOfInputOrder() {
        List<Detection> chronological =
                List.of(
                        detection("adsb-1", "ADSB", START, 0),
                        detection("radar-1", "RADAR", START.plusSeconds(1), 1),
                        detection("adsb-1", "ADSB", START.plusSeconds(2), 2));
        List<Detection> shuffled = new ArrayList<>(chronological);
        java.util.Collections.reverse(shuffled);

        List<TrackSnapshot> first = new FusionEngine(PLANE).replay(chronological);
        List<TrackSnapshot> second = new FusionEngine(PLANE).replay(shuffled);

        assertThat(second).isEqualTo(first);
        assertThat(first)
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.status()).isEqualTo(TrackStatus.CONFIRMED);
                            assertThat(track.contributors()).hasSize(2);
                        });
    }

    private static Detection detection(
            String sourceId, String sourceType, Instant observedAt, double eastMeters) {
        GeodeticPoint point = PLANE.toGeodetic(new EnuPoint(eastMeters, 0, 0));
        return new Detection(
                sourceId,
                sourceType,
                observedAt,
                observedAt.plusMillis(20),
                point.latDeg(),
                point.lonDeg(),
                point.altMeters(),
                1.0,
                90.0,
                5,
                Map.of());
    }
}
