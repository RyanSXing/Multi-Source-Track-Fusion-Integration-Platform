package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.EnuPoint;
import com.ryanxing.trackfusion.common.GeodeticPoint;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.common.RadarPacket;
import com.ryanxing.trackfusion.common.RadarPacketCodec;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import reactor.core.publisher.Flux;

public final class RadarSimulator {
    private final RadarSimulationConfig config;
    private final Random random;
    private final AtomicLong falseTargetId = new AtomicLong();

    public RadarSimulator(RadarSimulationConfig config, long seed) {
        this.config = Objects.requireNonNull(config, "config");
        random = new Random(seed);
    }

    public java.time.Duration latency() {
        return config.latency();
    }

    public synchronized List<RadarPacket> simulate(Detection truth) {
        Objects.requireNonNull(truth, "truth");
        List<RadarPacket> packets = new ArrayList<>(2);
        if (random.nextDouble() >= config.dropoutRate()) {
            packets.add(packet(truth, groundTruthId(truth), config.positionNoiseMeters()));
        }
        if (random.nextDouble() < config.falseTargetRate()) {
            packets.add(
                    packet(
                            truth,
                            "false-" + falseTargetId.incrementAndGet(),
                            Math.max(1_000, config.positionNoiseMeters() * 10)));
        }
        return List.copyOf(packets);
    }

    public void transmit(Flux<Detection> truths, InetSocketAddress target) throws IOException {
        Objects.requireNonNull(truths, "truths");
        Objects.requireNonNull(target, "target");
        try (DatagramChannel channel = DatagramChannel.open()) {
            truths.delayElements(config.latency())
                    .concatMapIterable(this::simulate)
                    .doOnNext(packet -> send(channel, target, packet))
                    .blockLast();
        }
    }

    private RadarPacket packet(Detection truth, String groundTruthId, double noiseMeters) {
        double altitude = truth.altMeters() == null ? 0 : truth.altMeters();
        GeodeticPoint point;
        if (noiseMeters == 0) {
            point = new GeodeticPoint(truth.latDeg(), truth.lonDeg(), altitude);
        } else {
            LocalTangentPlane plane =
                    new LocalTangentPlane(truth.latDeg(), truth.lonDeg(), altitude);
            point =
                    plane.toGeodetic(
                            new EnuPoint(
                                    random.nextGaussian() * noiseMeters,
                                    random.nextGaussian() * noiseMeters,
                                    0));
        }
        return new RadarPacket(
                config.sourceId(),
                groundTruthId,
                truth.observedAt(),
                point.latDeg(),
                point.lonDeg(),
                point.altMeters(),
                truth.speedMps() == null ? 0 : truth.speedMps(),
                truth.headingDeg() == null ? 0 : truth.headingDeg(),
                Math.max(1, config.positionNoiseMeters()));
    }

    private static String groundTruthId(Detection truth) {
        return truth.attributes()
                .getOrDefault(
                        "icao24",
                        truth.attributes().getOrDefault(
                                "mmsi", truth.sourceId() + '-' + truth.observedAt().toEpochMilli()));
    }

    private static void send(
            DatagramChannel channel, InetSocketAddress target, RadarPacket packet) {
        try {
            channel.send(ByteBuffer.wrap(RadarPacketCodec.encode(packet)), target);
        } catch (IOException sendFailure) {
            throw new UncheckedIOException(sendFailure);
        }
    }
}
