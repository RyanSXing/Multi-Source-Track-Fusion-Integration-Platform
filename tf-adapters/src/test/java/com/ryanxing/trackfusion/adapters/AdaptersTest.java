package com.ryanxing.trackfusion.adapters;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.RadarPacket;
import com.ryanxing.trackfusion.common.RadarPacketCodec;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class AdaptersTest {
    private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");

    @Test
    void fixtureAdapterIsAReusableColdFlux() {
        FixtureSourceAdapter adapter =
                new FixtureSourceAdapter(
                        "fixture", "ADSB", List.of(detection("fixture", "ADSB")));

        StepVerifier.create(adapter.stream()).expectNextCount(1).verifyComplete();
        StepVerifier.create(adapter.stream()).expectNextCount(1).verifyComplete();
    }

    @Test
    void replayHonorsObservedTimeAtTheConfiguredMultiplier() {
        Detection first = detection("fixture", "ADSB");
        Detection second =
                new Detection(
                        "fixture",
                        "ADSB",
                        NOW.plusSeconds(1),
                        NOW.plusSeconds(1),
                        43.65,
                        -79.38,
                        100.0,
                        10.0,
                        90.0,
                        10,
                        Map.of());
        ReplaySourceAdapter adapter =
                new ReplaySourceAdapter("fixture", "ADSB", List.of(second, first), 2);

        StepVerifier.withVirtualTime(adapter::stream)
                .expectSubscription()
                .expectNext(first)
                .expectNoEvent(Duration.ofMillis(499))
                .thenAwait(Duration.ofMillis(1))
                .expectNext(second)
                .verifyComplete();
    }

    @Test
    void parsesOpenSkyStateVectorsIntoTheCanonicalSchema() {
        String json =
                """
                {"states":[
                  ["c0ffee","AC 123 ","Canada",1767323040,1767323041,-79.38,43.65,
                   1200.0,false,90.5,181.0,0.0,null,1250.0,null,false,0],
                  ["bad",null,null,null,null,null,null,null,false,null,null,null,null,null,null,false,0]
                ]}
                """;

        List<Detection> detections =
                OpenSkyAdapter.parse("opensky", json, NOW, 12);

        assertThat(detections)
                .singleElement()
                .satisfies(
                        detection -> {
                            assertThat(detection.sourceId()).isEqualTo("opensky");
                            assertThat(detection.sourceType()).isEqualTo("ADSB");
                            assertThat(detection.observedAt())
                                    .isEqualTo(Instant.ofEpochSecond(1_767_323_040L));
                            assertThat(detection.altMeters()).isEqualTo(1250);
                            assertThat(detection.attributes())
                                    .containsEntry("icao24", "c0ffee")
                                    .containsEntry("callsign", "AC 123");
                        });
    }

    @Test
    void reconnectsAisAndParsesPositionReports() {
        String json =
                """
                {
                  "MessageType":"PositionReport",
                  "MetaData":{
                    "MMSI":316001234,
                    "ShipName":"TEST SHIP",
                    "time_utc":"2026-01-02T03:04:05Z"
                  },
                  "Message":{"PositionReport":{
                    "Latitude":43.64,
                    "Longitude":-79.35,
                    "Sog":12.0,
                    "Cog":91.0
                  }}
                }
                """;
        AtomicInteger attempts = new AtomicInteger();
        AisStreamAdapter adapter =
                new AisStreamAdapter(
                        "aisstream",
                        () ->
                                attempts.getAndIncrement() == 0
                                        ? Flux.error(new IllegalStateException("disconnect"))
                                        : Flux.just(json),
                        Duration.ofMillis(1),
                        Duration.ofMillis(2),
                        25);

        StepVerifier.create(adapter.stream())
                .assertNext(
                        detection -> {
                            assertThat(detection.sourceType()).isEqualTo("AIS");
                            assertThat(detection.speedMps()).isCloseTo(6.173, within(0.001));
                            assertThat(detection.attributes())
                                    .containsEntry("mmsi", "316001234")
                                    .containsEntry("shipName", "TEST SHIP");
                        })
                .verifyComplete();
        assertThat(attempts).hasValue(2);
    }

    @Test
    void cachesWeatherByRoundedGridCell() {
        AtomicInteger calls = new AtomicInteger();
        Function<URI, CompletableFuture<String>> fetcher =
                ignored -> {
                    calls.incrementAndGet();
                    return CompletableFuture.completedFuture(
                            """
                            {"current":{
                              "temperature_2m":21.5,
                              "wind_speed_10m":18.0,
                              "wind_direction_10m":270,
                              "weather_code":2
                            }}
                            """);
                };
        OpenMeteoEnricher enricher =
                new OpenMeteoEnricher(
                        fetcher,
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        Duration.ofMinutes(10),
                        0.1);

        Detection first = enricher.enrich(detection("fixture", "ADSB")).block();
        Detection second =
                enricher.enrich(
                                new Detection(
                                        "fixture",
                                        "ADSB",
                                        NOW,
                                        NOW,
                                        43.651,
                                        -79.381,
                                        100.0,
                                        10.0,
                                        90.0,
                                        10,
                                        Map.of()))
                        .block();

        assertThat(calls).hasValue(1);
        assertThat(first.attributes())
                .containsEntry("weather.temperatureC", "21.5")
                .containsEntry("weather.windSpeedMps", "5.0");
        assertThat(second.attributes()).containsEntry("weather.code", "2");
    }

    @Test
    void circuitBreakerStopsCallingAFailedSource() {
        SourceAdapter failing =
                new SourceAdapter() {
                    @Override
                    public String sourceId() {
                        return "failed";
                    }

                    @Override
                    public String sourceType() {
                        return "TEST";
                    }

                    @Override
                    public Flux<Detection> stream() {
                        return Flux.error(new IllegalStateException("down"));
                    }
                };
        CircuitBreaker breaker =
                CircuitBreaker.of(
                        "failed",
                        CircuitBreakerConfig.custom()
                                .minimumNumberOfCalls(2)
                                .slidingWindowSize(2)
                                .failureRateThreshold(100)
                                .build());
        SourceAdapter guarded = new ResilientSourceAdapter(failing, breaker);

        StepVerifier.create(guarded.stream())
                .expectError(IllegalStateException.class)
                .verify();
        StepVerifier.create(guarded.stream())
                .expectError(IllegalStateException.class)
                .verify();
        StepVerifier.create(guarded.stream())
                .expectError(CallNotPermittedException.class)
                .verify();
    }

    @Test
    void radarDatagramsDecodeOnlyForTheConfiguredSensor() {
        RadarPacket packet =
                new RadarPacket(
                        "radar-east",
                        "aircraft-c0ffee",
                        NOW,
                        43.65,
                        -79.38,
                        1200,
                        90,
                        180,
                        25);
        byte[] encoded = RadarPacketCodec.encode(packet);

        Detection detection = RadarUdpAdapter.decode("radar-east", encoded, NOW.plusMillis(50));

        assertThat(detection.sourceType()).isEqualTo("RADAR");
        assertThat(detection.attributes())
                .containsEntry("groundTruthId", "aircraft-c0ffee");
        assertThatIllegalArgumentException()
                .isThrownBy(() -> RadarUdpAdapter.decode("radar-west", encoded, NOW));
    }

    @Test
    void receivesRadarPacketsOverUdp() throws Exception {
        int port;
        try (DatagramSocket reservation = new DatagramSocket(0)) {
            port = reservation.getLocalPort();
        }
        RadarPacket packet =
                new RadarPacket(
                        "radar-east",
                        "aircraft-c0ffee",
                        NOW,
                        43.65,
                        -79.38,
                        1200,
                        90,
                        180,
                        25);
        byte[] bytes = RadarPacketCodec.encode(packet);
        RadarUdpAdapter adapter = new RadarUdpAdapter("radar-east", port);

        StepVerifier.create(adapter.stream().take(1))
                .then(
                        () -> {
                            try (DatagramSocket sender = new DatagramSocket()) {
                                sender.send(
                                        new DatagramPacket(
                                                bytes,
                                                bytes.length,
                                                InetAddress.getLoopbackAddress(),
                                                port));
                            } catch (java.io.IOException failure) {
                                throw new java.io.UncheckedIOException(failure);
                            }
                        })
                .assertNext(
                        detection ->
                                assertThat(detection.attributes())
                                        .containsEntry(
                                                "groundTruthId", "aircraft-c0ffee"))
                .verifyComplete();
    }

    @Test
    void radarSimulatorAppliesDeterministicDropoutAndFalseTargets() {
        Detection truth =
                new Detection(
                        "opensky",
                        "ADSB",
                        NOW,
                        NOW,
                        43.65,
                        -79.38,
                        1200.0,
                        90.0,
                        180.0,
                        10,
                        Map.of("icao24", "c0ffee"));
        RadarSimulator clean =
                new RadarSimulator(
                        new RadarSimulationConfig(
                                "radar-east", 0, 0, Duration.ZERO, 0),
                        7);
        RadarSimulator noisy =
                new RadarSimulator(
                        new RadarSimulationConfig(
                                "radar-east", 50, 0, Duration.ofMillis(20), 1),
                        7);
        RadarSimulator dropped =
                new RadarSimulator(
                        new RadarSimulationConfig(
                                "radar-east", 50, 1, Duration.ZERO, 0),
                        7);

        assertThat(clean.simulate(truth))
                .singleElement()
                .satisfies(
                        packet -> {
                            assertThat(packet.groundTruthId()).isEqualTo("c0ffee");
                            assertThat(packet.latDeg()).isEqualTo(truth.latDeg());
                            assertThat(packet.lonDeg()).isEqualTo(truth.lonDeg());
                        });
        assertThat(noisy.simulate(truth))
                .hasSize(2)
                .extracting(RadarPacket::groundTruthId)
                .anyMatch(id -> id.startsWith("false-"));
        assertThat(dropped.simulate(truth)).isEmpty();
        assertThat(noisy.latency()).isEqualTo(Duration.ofMillis(20));
    }

    @Test
    void radarSimulatorProcessParsesRecordedAdsbCsv() {
        List<Detection> detections =
                RadarSimulatorMain.parse(
                        List.of(
                                "observedAt,latDeg,lonDeg,altMeters,speedMps,headingDeg,icao24",
                                "2026-01-02T03:04:05Z,43.65,-79.38,1200,90,180,c0ffee"));

        assertThat(detections)
                .singleElement()
                .satisfies(
                        detection -> {
                            assertThat(detection.sourceId()).isEqualTo("opensky-replay");
                            assertThat(detection.attributes())
                                    .containsEntry("icao24", "c0ffee");
                        });
    }

    private static Detection detection(String sourceId, String sourceType) {
        return new Detection(
                sourceId,
                sourceType,
                NOW,
                NOW,
                43.65,
                -79.38,
                100.0,
                10.0,
                90.0,
                10,
                Map.of());
    }

    private static org.assertj.core.data.Offset<Double> within(double value) {
        return org.assertj.core.data.Offset.offset(value);
    }
}
