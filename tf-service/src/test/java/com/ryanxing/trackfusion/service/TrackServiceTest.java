package com.ryanxing.trackfusion.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.fusion.FusionConfig;
import com.ryanxing.trackfusion.fusion.FusionEngine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

class TrackServiceTest {
    private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");

    @Test
    void fusesPersistsAndBroadcastsOneTrackPerTick() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        TrackHistoryRepository repository = repository(saved);
        TrackService service = service(repository);

        StepVerifier.create(service.updates().take(1))
                .then(
                        () ->
                                service.process(
                                                NOW.plusSeconds(1),
                                                List.of(
                                                        detection(
                                                                "opensky",
                                                                "ADSB",
                                                                43.6500,
                                                                -79.3800),
                                                        detection(
                                                                "radar-east",
                                                                "RADAR",
                                                                43.6500,
                                                                -79.3800)))
                                        .block())
                .assertNext(
                        track -> {
                            assertThat(track.trackId()).isEqualTo(1);
                            assertThat(track.contributors()).hasSize(2);
                        })
                .verifyComplete();

        assertThat(saved.get()).singleElement();
        assertThat(service.currentTracks()).singleElement();
    }

    @Test
    void exposesCurrentHistoryAndSourceHealthApis() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        TrackHistoryRepository repository = repository(saved);
        TrackService service = service(repository);
        service.process(
                        NOW.plusSeconds(1),
                        List.of(detection("opensky", "ADSB", 43.65, -79.38)))
                .block();
        SourceHealthRegistry health =
                new SourceHealthRegistry(
                        Clock.fixed(NOW.plusSeconds(2), ZoneOffset.UTC),
                        new SimpleMeterRegistry());
        health.message(detection("opensky", "ADSB", 43.65, -79.38));
        WebTestClient client =
                WebTestClient.bindToController(new TrackController(service, health)).build();

        client.get()
                .uri("/api/tracks")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$[0].trackId")
                .isEqualTo(1);
        client.get()
                .uri("/api/tracks/1/history")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$[0].trackId")
                .isEqualTo(1);
        client.get()
                .uri("/api/sources/health")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$[0].sourceId")
                .isEqualTo("opensky");
    }

    @Test
    void kafkaBatchesAdvanceFusionAndSourceHealthOnStrictTicks() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        TrackService service = service(repository(saved));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        Clock clock = Clock.fixed(NOW.plusSeconds(1), ZoneOffset.UTC);
        SourceHealthRegistry health = new SourceHealthRegistry(clock, meters);
        DetectionBatchConsumer consumer =
                new DetectionBatchConsumer(service, health, clock);
        Detection detection = detection("opensky", "ADSB", 43.65, -79.38);

        consumer.consume(List.of(detection));
        consumer.consume(List.of(detection));

        assertThat(service.currentTracks())
                .singleElement()
                .satisfies(track -> assertThat(track.hitCount()).isEqualTo(2));
        assertThat(health.snapshots())
                .singleElement()
                .satisfies(source -> assertThat(source.messageCount()).isEqualTo(2));
    }

    @SuppressWarnings("unchecked")
    private static TrackHistoryRepository repository(
            AtomicReference<List<TrackHistoryEntity>> saved) {
        return (TrackHistoryRepository)
                Proxy.newProxyInstance(
                        TrackHistoryRepository.class.getClassLoader(),
                        new Class<?>[] {TrackHistoryRepository.class},
                        (proxy, method, arguments) -> {
                            if ("saveAll".equals(method.getName())) {
                                List<TrackHistoryEntity> rows =
                                        Flux.fromIterable(
                                                        (Iterable<TrackHistoryEntity>)
                                                                arguments[0])
                                                .collectList()
                                                .block();
                                saved.set(rows);
                                return Flux.fromIterable(rows);
                            }
                            if ("findByTrackIdOrderByStateAtAsc".equals(
                                    method.getName())) {
                                long trackId = (long) arguments[0];
                                return Flux.fromIterable(saved.get())
                                        .filter(row -> row.trackId() == trackId);
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static TrackService service(TrackHistoryRepository repository) {
        LocalTangentPlane plane = new LocalTangentPlane(43.65, -79.38, 0);
        return new TrackService(
                new FusionEngine(
                        plane,
                        new FusionConfig(1, 1, 3, 9.21, 1)),
                plane,
                repository,
                new ObjectMapper().findAndRegisterModules(),
                new SimpleMeterRegistry());
    }

    private static Detection detection(
            String sourceId, String sourceType, double latitude, double longitude) {
        return new Detection(
                sourceId,
                sourceType,
                NOW,
                NOW.plusMillis(20),
                latitude,
                longitude,
                100.0,
                10.0,
                90.0,
                10,
                Map.of());
    }
}
