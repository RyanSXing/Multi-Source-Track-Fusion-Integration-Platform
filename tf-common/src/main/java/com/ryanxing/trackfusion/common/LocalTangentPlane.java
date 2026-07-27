package com.ryanxing.trackfusion.common;

public final class LocalTangentPlane {
    private static final double WGS84_A = 6_378_137.0;
    private static final double WGS84_F = 1.0 / 298.257_223_563;
    private static final double WGS84_B = WGS84_A * (1 - WGS84_F);
    private static final double E2 = WGS84_F * (2 - WGS84_F);
    private static final double EP2 =
            (WGS84_A * WGS84_A - WGS84_B * WGS84_B) / (WGS84_B * WGS84_B);

    private final Ecef origin;
    private final double sinLat;
    private final double cosLat;
    private final double sinLon;
    private final double cosLon;

    public LocalTangentPlane(double latDeg, double lonDeg, double altMeters) {
        GeodeticPoint point = new GeodeticPoint(latDeg, lonDeg, altMeters);
        origin = toEcef(point);
        double lat = Math.toRadians(latDeg);
        double lon = Math.toRadians(lonDeg);
        sinLat = Math.sin(lat);
        cosLat = Math.cos(lat);
        sinLon = Math.sin(lon);
        cosLon = Math.cos(lon);
    }

    public EnuPoint toEnu(double latDeg, double lonDeg, double altMeters) {
        Ecef point = toEcef(new GeodeticPoint(latDeg, lonDeg, altMeters));
        double x = point.x - origin.x;
        double y = point.y - origin.y;
        double z = point.z - origin.z;
        return new EnuPoint(
                -sinLon * x + cosLon * y,
                -sinLat * cosLon * x - sinLat * sinLon * y + cosLat * z,
                cosLat * cosLon * x + cosLat * sinLon * y + sinLat * z);
    }

    public GeodeticPoint toGeodetic(EnuPoint point) {
        double x =
                origin.x
                        - sinLon * point.eastMeters()
                        - sinLat * cosLon * point.northMeters()
                        + cosLat * cosLon * point.upMeters();
        double y =
                origin.y
                        + cosLon * point.eastMeters()
                        - sinLat * sinLon * point.northMeters()
                        + cosLat * sinLon * point.upMeters();
        double z =
                origin.z
                        + cosLat * point.northMeters()
                        + sinLat * point.upMeters();
        return fromEcef(new Ecef(x, y, z));
    }

    private static Ecef toEcef(GeodeticPoint point) {
        double lat = Math.toRadians(point.latDeg());
        double lon = Math.toRadians(point.lonDeg());
        double sinLat = Math.sin(lat);
        double radius = WGS84_A / Math.sqrt(1 - E2 * sinLat * sinLat);
        return new Ecef(
                (radius + point.altMeters()) * Math.cos(lat) * Math.cos(lon),
                (radius + point.altMeters()) * Math.cos(lat) * Math.sin(lon),
                (radius * (1 - E2) + point.altMeters()) * sinLat);
    }

    private static GeodeticPoint fromEcef(Ecef point) {
        double horizontal = Math.hypot(point.x, point.y);
        if (horizontal < 1e-9) {
            return new GeodeticPoint(
                    Math.copySign(90, point.z), 0, Math.abs(point.z) - WGS84_B);
        }

        double longitude = Math.atan2(point.y, point.x);
        double theta = Math.atan2(point.z * WGS84_A, horizontal * WGS84_B);
        double sinTheta = Math.sin(theta);
        double cosTheta = Math.cos(theta);
        double latitude =
                Math.atan2(
                        point.z + EP2 * WGS84_B * sinTheta * sinTheta * sinTheta,
                        horizontal - E2 * WGS84_A * cosTheta * cosTheta * cosTheta);
        double sinLatitude = Math.sin(latitude);
        double radius = WGS84_A / Math.sqrt(1 - E2 * sinLatitude * sinLatitude);
        double altitude = horizontal / Math.cos(latitude) - radius;
        return new GeodeticPoint(
                Math.toDegrees(latitude), Math.toDegrees(longitude), altitude);
    }

    private record Ecef(double x, double y, double z) {}
}
