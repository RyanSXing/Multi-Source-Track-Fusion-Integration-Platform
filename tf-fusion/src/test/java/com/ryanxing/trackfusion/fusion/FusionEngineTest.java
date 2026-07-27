package com.ryanxing.trackfusion.fusion;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.EnuPoint;
import com.ryanxing.trackfusion.common.GeodeticPoint;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.common.RadarPacket;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FusionEngineTest {
    private static final LocalTangentPlane PLANE = new LocalTangentPlane(0, 0, 0);
    private static final Instant START = Instant.parse("2026-01-02T03:04:05Z");

    @Test
    void startsTrackIdsAfterPersistedHistory() {
        FusionEngine engine = new FusionEngine(PLANE, FusionConfig.defaults(), 42);

        assertThat(engine.update(START, List.of(detection("radar", "RADAR", START, 0))))
                .singleElement()
                .extracting(TrackSnapshot::trackId)
                .isEqualTo(42L);
    }

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
    void gatesAndLeavesRectangularAssignmentsUnmatched() {
        assertThat(
                        HungarianAssignment.solve(
                                new double[][] {
                                    {1, Double.POSITIVE_INFINITY},
                                    {2, 1},
                                    {100, 2}
                                },
                                10))
                .containsExactly(0, 1, -1);
        assertThat(HungarianAssignment.solve(new double[][] {{9.21}}, 9.21))
                .containsExactly(0);
        assertThat(HungarianAssignment.solve(new double[][] {{9.2101}}, 9.21))
                .containsExactly(-1);
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

    @Test
    void assignsEachSourceGloballyButCountsOneLifecycleHitPerTick() {
        FusionEngine engine =
                new FusionEngine(PLANE, new FusionConfig(2, 3, 3, 9.21, 1));

        List<TrackSnapshot> newborn =
                engine.update(
                        START,
                        List.of(
                                detection("adsb-feed", "ADSB", START, 0),
                                detection("radar-a", "RADAR", START, 0)));

        assertThat(newborn)
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.status()).isEqualTo(TrackStatus.TENTATIVE);
                            assertThat(track.hitCount()).isEqualTo(1);
                            assertThat(track.contributors()).hasSize(2);
                        });

        FusionEngine twoTargets =
                new FusionEngine(PLANE, new FusionConfig(2, 3, 3, 9.21, 1));
        twoTargets.update(
                START,
                List.of(
                        detection("adsb-feed", "ADSB", START, 0),
                        detection("adsb-feed", "ADSB", START, 10)));
        List<TrackSnapshot> updated =
                twoTargets.update(
                        START.plusSeconds(1),
                        List.of(
                                detection(
                                        "radar-a", "RADAR", START.plusSeconds(1), 0),
                                detection(
                                        "radar-b", "RADAR", START.plusSeconds(1), 1)));

        assertThat(updated).hasSize(2);
        assertThat(updated.get(0).contributors()).hasSize(3);
        assertThat(updated.get(0).status()).isEqualTo(TrackStatus.CONFIRMED);
        assertThat(updated.get(1).contributors()).hasSize(1);
    }

    @Test
    void coalescesRepeatedObjectUpdatesFromOneSourceWithinATick() {
        FusionEngine engine =
                new FusionEngine(PLANE, new FusionConfig(1, 1, 3, 9.21, 1));
        Instant tick = START.plusSeconds(1);

        List<TrackSnapshot> tracks =
                engine.updateAt(
                        tick,
                        List.of(
                                detection(
                                        "adsb-feed",
                                        "ADSB",
                                        START.plusMillis(100),
                                        0,
                                        10.0,
                                        90.0,
                                        5,
                                        Map.of("icao24", "abc123")),
                                detection(
                                        "adsb-feed",
                                        "ADSB",
                                        START.plusMillis(800),
                                        4,
                                        10.0,
                                        90.0,
                                        5,
                                        Map.of("icao24", "abc123")),
                                radar(START.plusMillis(200), 0),
                                radar(START.plusMillis(700), 4)));

        assertThat(tracks)
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.contributors()).hasSize(2);
                            assertThat(track.contributors())
                                    .filteredOn(detection -> detection.sourceType().equals("ADSB"))
                                    .singleElement()
                                    .extracting(Detection::observedAt)
                                    .isEqualTo(START.plusMillis(800));
                            assertThat(track.contributors())
                                    .filteredOn(detection -> detection.sourceType().equals("RADAR"))
                                    .singleElement()
                                    .extracting(Detection::observedAt)
                                    .isEqualTo(START.plusMillis(700));
                        });
    }

    @Test
    void replayOrdersComparatorTiesByEveryStateAffectingField() {
        Detection precise =
                detection(
                        "adsb-feed",
                        "ADSB",
                        START,
                        0,
                        2.0,
                        90.0,
                        1,
                        Map.of("target", "precise"));
        Detection noisy =
                detection(
                        "adsb-feed",
                        "ADSB",
                        START,
                        0,
                        20.0,
                        270.0,
                        10,
                        Map.of("target", "noisy"));

        List<TrackSnapshot> first =
                new FusionEngine(PLANE).replay(List.of(precise, noisy));
        List<TrackSnapshot> second =
                new FusionEngine(PLANE).replay(List.of(noisy, precise));

        assertThat(second).isEqualTo(first);
    }

    @Test
    void doesNotInventVelocityWhenHeadingIsMissing() {
        FusionEngine engine = new FusionEngine(PLANE);

        engine.update(
                START,
                List.of(
                        detection(
                                "adsb-feed",
                                "ADSB",
                                START,
                                0,
                                100.0,
                                null,
                                5,
                                Map.of())));

        assertThat(
                        engine.update(
                                START.plusSeconds(1),
                                List.of(
                                        detection(
                                                "adsb-feed",
                                                "ADSB",
                                                START.plusSeconds(1),
                                                0,
                                                100.0,
                                                null,
                                                5,
                                                Map.of()))))
                .hasSize(1);
    }

    @Test
    void rejectsRepeatedTicksAndMismatchedMeasurementTimes() {
        FusionEngine engine = new FusionEngine(PLANE);
        engine.update(START, List.of(detection("adsb-feed", "ADSB", START, 0)));

        assertThatIllegalArgumentException()
                .isThrownBy(() -> engine.update(START, List.of()));
        assertThatIllegalArgumentException()
                .isThrownBy(
                        () ->
                                engine.update(
                                        START.plusSeconds(1),
                                List.of(
                                        detection(
                                                "adsb-feed",
                                                "ADSB",
                                                START,
                                                0))));
    }

    @Test
    void acceptsBufferedObservationsOnALaterServiceTick() {
        FusionEngine engine =
                new FusionEngine(PLANE, new FusionConfig(1, 1, 3, 9.21, 1));

        assertThat(
                        engine.updateAt(
                                START.plusSeconds(1),
                                List.of(
                                        detection("adsb-feed", "ADSB", START, 0),
                                        detection("radar-east", "RADAR", START, 0))))
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.stateAt()).isEqualTo(START.plusSeconds(1));
                            assertThat(track.lastObservedAt()).isEqualTo(START);
                            assertThat(track.contributors()).hasSize(2);
                        });
    }

    @Test
    void dropsTentativeTracksThatMissTheConfirmationWindow() {
        FusionEngine engine =
                new FusionEngine(PLANE, new FusionConfig(3, 3, 5, 9.21, 1));
        engine.update(START, List.of(detection("adsb-feed", "ADSB", START, 0)));
        engine.update(START.plusSeconds(1), List.of());

        assertThat(engine.update(START.plusSeconds(2), List.of()))
                .singleElement()
                .extracting(TrackSnapshot::status)
                .isEqualTo(TrackStatus.DROPPED);
        assertThat(engine.tracks()).isEmpty();
    }

    private static Detection detection(
            String sourceId, String sourceType, Instant observedAt, double eastMeters) {
        return detection(
                sourceId,
                sourceType,
                observedAt,
                eastMeters,
                1.0,
                90.0,
                5,
                Map.of());
    }

    private static Detection detection(
            String sourceId,
            String sourceType,
            Instant observedAt,
            double eastMeters,
            Double speedMps,
            Double headingDeg,
            double sigmaMeters,
            Map<String, String> attributes) {
        GeodeticPoint point = PLANE.toGeodetic(new EnuPoint(eastMeters, 0, 0));
        return new Detection(
                sourceId,
                sourceType,
                observedAt,
                observedAt.plusMillis(20),
                point.latDeg(),
                point.lonDeg(),
                point.altMeters(),
                speedMps,
                headingDeg,
                sigmaMeters,
                attributes);
    }

    private static Detection radar(Instant observedAt, double eastMeters) {
        GeodeticPoint point = PLANE.toGeodetic(new EnuPoint(eastMeters, 0, 0));
        return new RadarPacket(
                        "radar-feed",
                        "abc123",
                        observedAt,
                        point.latDeg(),
                        point.lonDeg(),
                        point.altMeters(),
                        10,
                        90,
                        25)
                .toDetection(observedAt.plusMillis(20));
    }
}
