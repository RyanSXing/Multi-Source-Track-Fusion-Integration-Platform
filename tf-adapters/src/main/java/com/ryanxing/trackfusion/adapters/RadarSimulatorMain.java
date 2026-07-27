package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public final class RadarSimulatorMain {
    private RadarSimulatorMain() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            throw new IllegalArgumentException(
                    "usage: RadarSimulatorMain <host> <port> <adsb-replay.csv>");
        }
        List<Detection> truths = parse(Files.readAllLines(Path.of(args[2])));
        double replaySpeed = environmentDouble("RADAR_REPLAY_SPEED", 1);
        RadarSimulationConfig config =
                new RadarSimulationConfig(
                        System.getenv().getOrDefault("RADAR_SOURCE_ID", "radar-east"),
                        environmentDouble("RADAR_POSITION_NOISE_METERS", 50),
                        environmentDouble("RADAR_DROPOUT_RATE", 0.05),
                        Duration.ofMillis((long) environmentDouble("RADAR_LATENCY_MS", 100)),
                        environmentDouble("RADAR_FALSE_TARGET_RATE", 0.01));
        RadarSimulator simulator =
                new RadarSimulator(config, (long) environmentDouble("RADAR_RANDOM_SEED", 7));
        simulator.transmit(
                new ReplaySourceAdapter(
                                "opensky-replay", "ADSB", truths, replaySpeed)
                        .stream(),
                new InetSocketAddress(args[0], Integer.parseInt(args[1])));
    }

    static List<Detection> parse(List<String> lines) {
        if (lines == null || lines.size() < 2) {
            throw new IllegalArgumentException("radar replay CSV has no data");
        }
        List<Detection> detections = new ArrayList<>();
        for (int lineNumber = 1; lineNumber < lines.size(); lineNumber++) {
            String line = lines.get(lineNumber);
            if (line.isBlank()) {
                continue;
            }
            String[] values = line.split(",", -1);
            if (values.length != 7) {
                throw new IllegalArgumentException(
                        "invalid radar replay CSV line " + (lineNumber + 1));
            }
            Instant observedAt = Instant.parse(values[0].trim());
            detections.add(
                    new Detection(
                            "opensky-replay",
                            "ADSB",
                            observedAt,
                            observedAt,
                            Double.parseDouble(values[1].trim()),
                            Double.parseDouble(values[2].trim()),
                            Double.parseDouble(values[3].trim()),
                            Double.parseDouble(values[4].trim()),
                            Double.parseDouble(values[5].trim()),
                            10,
                            Map.of("icao24", values[6].trim())));
        }
        return List.copyOf(detections);
    }

    private static double environmentDouble(String name, double fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Double.parseDouble(value);
    }
}
