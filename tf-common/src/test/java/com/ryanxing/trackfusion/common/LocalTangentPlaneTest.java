package com.ryanxing.trackfusion.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import org.junit.jupiter.api.Test;

class LocalTangentPlaneTest {
    private static final double MILLIMETER = 0.001;

    @Test
    void convertsWgs84ToLocalEastNorthUp() {
        LocalTangentPlane plane = new LocalTangentPlane(0, 0, 0);

        EnuPoint origin = plane.toEnu(0, 0, 0);
        EnuPoint east = plane.toEnu(0, 0.001, 0);
        EnuPoint north = plane.toEnu(0.001, 0, 0);

        assertThat(origin.eastMeters()).isCloseTo(0, within(MILLIMETER));
        assertThat(origin.northMeters()).isCloseTo(0, within(MILLIMETER));
        assertThat(origin.upMeters()).isCloseTo(0, within(MILLIMETER));
        assertThat(east.eastMeters()).isCloseTo(111.319, within(0.01));
        assertThat(east.northMeters()).isCloseTo(0, within(MILLIMETER));
        assertThat(north.northMeters()).isCloseTo(110.574, within(0.01));
        assertThat(north.eastMeters()).isCloseTo(0, within(MILLIMETER));
    }

    @Test
    void roundTripsThroughEnu() {
        LocalTangentPlane plane = new LocalTangentPlane(43.6532, -79.3832, 76);

        GeodeticPoint expected = new GeodeticPoint(43.6719, -79.4023, 350);
        GeodeticPoint actual =
                plane.toGeodetic(
                        plane.toEnu(
                                expected.latDeg(), expected.lonDeg(), expected.altMeters()));

        assertThat(actual.latDeg()).isCloseTo(expected.latDeg(), within(1e-9));
        assertThat(actual.lonDeg()).isCloseTo(expected.lonDeg(), within(1e-9));
        assertThat(actual.altMeters()).isCloseTo(expected.altMeters(), within(1e-4));
    }

    @Test
    void rejectsNonGeodeticInput() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LocalTangentPlane(91, 0, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LocalTangentPlane(0, 181, 0));
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new LocalTangentPlane(0, 0, Double.NaN));
    }

    private static org.assertj.core.data.Offset<Double> within(double value) {
        return org.assertj.core.data.Offset.offset(value);
    }
}
