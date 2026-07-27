package com.ryanxing.trackfusion.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.adapters.SourceAdapter;
import com.ryanxing.trackfusion.adapters.TrackEnricher;
import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.fusion.FusionConfig;
import com.ryanxing.trackfusion.fusion.FusionEngine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Proxy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
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
                                                NOW,
                                                List.of(
                                                        kafka(
                                                                0,
                                                                detection(
                                                                        "opensky",
                                                                        "ADSB",
                                                                        43.6500,
                                                                        -79.3800)),
                                                        kafka(
                                                                1,
                                                                detection(
                                                                        "radar-east",
                                                                        "RADAR",
                                                                        43.6500,
                                                                        -79.3800))))
                                        .block())
                .assertNext(
                        event -> {
                            assertThat(event.version()).isEqualTo(1);
                            assertThat(event.tracks()).singleElement()
                                    .satisfies(
                                            track -> {
                                                assertThat(track.trackId()).isEqualTo(1);
                                                assertThat(track.contributors()).hasSize(2);
                                            });
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
                        NOW,
                        List.of(
                                kafka(
                                        0,
                                        detection(
                                                "opensky",
                                                "ADSB",
                                                43.65,
                                                -79.38))))
                .block();
        SourceHealthRegistry health =
                new SourceHealthRegistry(
                        Clock.fixed(NOW.plusSeconds(2), ZoneOffset.UTC),
                        new SimpleMeterRegistry());
        health.consumed(detection("opensky", "ADSB", 43.65, -79.38));
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
                .uri("/api/session")
                .exchange()
                .expectStatus()
                .isOk()
                .expectBody()
                .jsonPath("$.sessionId")
                .isNotEmpty();
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
    void kafkaBatchesUseEventTimeAndIgnoreRedeliveredOffsets() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        TrackService service = service(repository(saved));
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        Clock clock = Clock.fixed(NOW.plusSeconds(10), ZoneOffset.UTC);
        SourceHealthRegistry health = new SourceHealthRegistry(clock, meters);
        DetectionBatchConsumer consumer =
                new DetectionBatchConsumer(
                        service,
                        new AdapterKafkaPublisher(
                                List.of(), List.of(), null, health, "detections"),
                        health,
                        clock,
                        Duration.ofSeconds(1),
                        Duration.ZERO);
        List<ConsumerRecord<String, Detection>> firstPoll =
                List.of(
                        record(
                                0,
                                detection(
                                        "opensky",
                                        "ADSB",
                                        NOW,
                                        43.65,
                                        -79.38)));
        List<ConsumerRecord<String, Detection>> secondPoll =
                List.of(
                        record(
                                1,
                                detection(
                                        "radar-east",
                                        "RADAR",
                                        NOW.plusMillis(100),
                                        43.65,
                                        -79.38)),
                        record(
                                2,
                                detection(
                                        "opensky",
                                        "ADSB",
                                        NOW.plusSeconds(1),
                                        43.6501,
                                        -79.38)));

        consumer.consume(firstPoll);
        consumer.consume(secondPoll);
        consumer.flush();
        consumer.consume(firstPoll);
        consumer.consume(secondPoll);
        consumer.consume(
                List.of(
                        record(
                                3,
                                detection(
                                        "opensky",
                                        "ADSB",
                                        NOW.minusSeconds(1),
                                        43.64,
                                        -79.38))));
        consumer.flush();

        assertThat(service.currentTracks())
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.hitCount()).isEqualTo(2);
                            assertThat(track.contributors()).hasSize(2);
                        });
        assertThat(health.snapshots())
                .filteredOn(source -> source.sourceType().equals("ADSB"))
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.kafkaConsumedCount()).isEqualTo(5);
                            assertThat(source.redeliveredCount()).isEqualTo(2);
                            assertThat(source.lateCount()).isEqualTo(1);
                        });
    }

    @Test
    void persistenceFailureIsRetriedWithoutApplyingTheTickTwice() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        AtomicBoolean failOnce = new AtomicBoolean(true);
        TrackService service = service(repository(saved, failOnce));
        List<TrackService.KafkaDetection> records =
                List.of(
                        kafka(
                                0,
                                detection(
                                        "opensky",
                                        "ADSB",
                                        43.65,
                                        -79.38)));

        StepVerifier.create(service.process(NOW, records))
                .expectErrorMessage("database unavailable")
                .verify();
        assertThat(service.currentTracks()).isEmpty();

        service.process(NOW, records).block();

        assertThat(service.currentTracks())
                .singleElement()
                .satisfies(track -> assertThat(track.hitCount()).isEqualTo(1));
        assertThat(saved.get()).singleElement();
    }

    @Test
    void failedTickRetriesItsOriginalMembershipBeforeNewArrivals() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        TrackService service =
                service(repository(saved, new AtomicBoolean(true)));
        TrackService.KafkaDetection first =
                kafka(0, detection("opensky", "ADSB", 43.65, -79.38));
        TrackService.KafkaDetection lateArrival =
                kafka(1, detection("radar-east", "RADAR", 43.65, -79.38));

        StepVerifier.create(service.process(NOW, List.of(first)))
                .expectErrorMessage("database unavailable")
                .verify();
        service.process(NOW, List.of(first, lateArrival)).block();
        TrackService.ProcessResult late =
                service.process(NOW, List.of(lateArrival)).block();

        assertThat(service.currentTracks())
                .singleElement()
                .satisfies(
                        track -> {
                            assertThat(track.hitCount()).isEqualTo(1);
                            assertThat(track.contributors())
                                    .extracting(Detection::sourceType)
                                    .containsExactly("ADSB");
                        });
        assertThat(late.lateIds()).containsExactly(lateArrival.id());
    }

    @Test
    void reconnectingWebSocketsReceiveAFullCurrentSnapshot() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(List.of());
        SimpleMeterRegistry meters = new SimpleMeterRegistry();
        TrackService service = service(repository(saved), meters);
        StepVerifier.create(service.updates(), 0)
                .then(
                        () ->
                                service.process(
                                                NOW,
                                                List.of(
                                                        kafka(
                                                                0,
                                                                detection(
                                                                        "opensky",
                                                                        "ADSB",
                                                                        43.65,
                                                                        -79.38))))
                                        .block())
                .thenCancel()
                .verify();

        TrackWebSocketHandler handler =
                new TrackWebSocketHandler(
                        service, new ObjectMapper().findAndRegisterModules());
        StepVerifier.create(handler.events(Duration.ofHours(1)).take(1))
                .assertNext(
                        event -> {
                            assertThat(event.snapshot()).isTrue();
                            assertThat(event.tracks()).hasSize(1);
                        })
                .verifyComplete();
    }

    @Test
    void continuesTrackIdsAfterPersistedHistory() {
        AtomicReference<List<TrackHistoryEntity>> saved =
                new AtomicReference<>(
                        List.of(
                                new TrackHistoryEntity(
                                        1L,
                                        "previous-session",
                                        41,
                                        "CONFIRMED",
                                        NOW.minusSeconds(1),
                                        NOW.minusSeconds(1),
                                        43.65,
                                        -79.38,
                                        100.0,
                                        0,
                                        0,
                                        3,
                                        0,
                                        "[]")));
        TrackService service = service(repository(saved));

        service.process(
                        NOW,
                        List.of(
                                kafka(
                                        0,
                                        detection(
                                                "opensky",
                                                "ADSB",
                                                43.65,
                                                -79.38))))
                .block();

        assertThat(service.currentTracks())
                .singleElement()
                .extracting(TrackView::trackId)
                .isEqualTo(42L);
    }

    @Test
    void kafkaPublishRetryCreatesANewSend() {
        AtomicInteger sends = new AtomicInteger();
        ProducerFactory<String, Detection> factory =
                () -> {
                    throw new UnsupportedOperationException();
                };
        KafkaTemplate<String, Detection> kafka =
                new KafkaTemplate<>(factory) {
                    @Override
                    public CompletableFuture<SendResult<String, Detection>> send(
                            String topic, String key, Detection value) {
                        return sends.getAndIncrement() == 0
                                ? CompletableFuture.failedFuture(
                                        new IllegalStateException("broker unavailable"))
                                : CompletableFuture.completedFuture(null);
                    }
                };
        SourceHealthRegistry health =
                new SourceHealthRegistry(
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        new SimpleMeterRegistry());
        AdapterKafkaPublisher publisher =
                new AdapterKafkaPublisher(
                        List.of(), List.of(), kafka, health, "detections");

        publisher.publish(detection("opensky", "ADSB", 43.65, -79.38))
                .block(Duration.ofSeconds(4));

        assertThat(sends).hasValue(2);
        assertThat(health.snapshots())
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.kafkaPublishedCount()).isEqualTo(1);
                            assertThat(source.errorCount()).isEqualTo(1);
                            assertThat(source.degraded()).isFalse();
                        });
    }

    @Test
    void successfulEnrichmentClearsOnlyItsCurrentFailure() {
        AtomicInteger enrichments = new AtomicInteger();
        TrackEnricher enricher =
                new TrackEnricher() {
                    @Override
                    public String sourceType() {
                        return "WEATHER";
                    }

                    @Override
                    public Mono<Detection> enrich(Detection detection) {
                        return enrichments.getAndIncrement() == 0
                                ? Mono.error(new IllegalStateException("weather unavailable"))
                                : Mono.just(detection);
                    }
                };
        KafkaTemplate<String, Detection> kafka =
                new KafkaTemplate<>(
                        (ProducerFactory<String, Detection>)
                                () -> {
                                    throw new UnsupportedOperationException();
                                }) {
                    @Override
                    public CompletableFuture<SendResult<String, Detection>> send(
                            String topic, String key, Detection value) {
                        return CompletableFuture.completedFuture(null);
                    }
                };
        SourceHealthRegistry health =
                new SourceHealthRegistry(
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        new SimpleMeterRegistry());
        AdapterKafkaPublisher publisher =
                new AdapterKafkaPublisher(
                        List.of(), List.of(enricher), kafka, health, "detections");

        publisher.publish(detection("opensky", "ADSB", 43.65, -79.38)).block();
        assertThat(health.snapshots())
                .filteredOn(source -> source.sourceType().equals("WEATHER"))
                .singleElement()
                .satisfies(source -> assertThat(source.degraded()).isTrue());

        publisher.publish(detection("opensky", "ADSB", 43.65, -79.38)).block();
        assertThat(health.snapshots())
                .filteredOn(source -> source.sourceType().equals("WEATHER"))
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.degraded()).isFalse();
                            assertThat(source.errorCount()).isEqualTo(1);
                        });
    }

    @Test
    void sourceAdaptersStartAndStopWithLeadershipState() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        SourceAdapter source =
                new SourceAdapter() {
                    @Override
                    public String sourceId() {
                        return "test-source";
                    }

                    @Override
                    public String sourceType() {
                        return "TEST";
                    }

                    @Override
                    public Flux<Detection> stream() {
                        return Flux.<Detection>never()
                                .doOnSubscribe(ignored -> subscriptions.incrementAndGet())
                                .doOnCancel(cancellations::incrementAndGet);
                    }
                };
        SourceHealthRegistry health =
                new SourceHealthRegistry(
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        new SimpleMeterRegistry());
        AdapterKafkaPublisher publisher =
                new AdapterKafkaPublisher(
                        List.of(source), List.of(), null, health, "detections");
        publisher.setActive(false);
        publisher.setActive(true);
        publisher.setActive(false);

        assertThat(subscriptions).hasValue(1);
        assertThat(cancellations).hasValue(1);
    }

    @Test
    void successfulStageClearsOnlyItsCurrentSourceDegradation() {
        SourceHealthRegistry health =
                new SourceHealthRegistry(
                        Clock.fixed(NOW, ZoneOffset.UTC),
                        new SimpleMeterRegistry());
        Detection detection = detection("opensky", "ADSB", 43.65, -79.38);

        health.error("opensky", "ADSB", "adapter");
        health.adapterReceived(detection);
        assertThat(health.snapshots())
                .singleElement()
                .satisfies(source -> assertThat(source.degraded()).isFalse());

        health.error("opensky", "ADSB", "publish");
        health.adapterReceived(detection);
        assertThat(health.snapshots())
                .singleElement()
                .satisfies(source -> assertThat(source.degraded()).isTrue());

        health.published(detection);

        assertThat(health.snapshots())
                .singleElement()
                .satisfies(
                        source -> {
                            assertThat(source.degraded()).isFalse();
                            assertThat(source.errorCount()).isEqualTo(2);
                        });
    }

    private static ConsumerRecord<String, Detection> record(
            long offset, Detection detection) {
        return new ConsumerRecord<>("detections", 0, offset, null, detection);
    }

    private static TrackService.KafkaDetection kafka(
            long offset, Detection detection) {
        return new TrackService.KafkaDetection(
                "detections", 0, offset, detection);
    }

    @SuppressWarnings("unchecked")
    private static TrackHistoryRepository repository(
            AtomicReference<List<TrackHistoryEntity>> saved) {
        return repository(saved, new AtomicBoolean());
    }

    @SuppressWarnings("unchecked")
    private static TrackHistoryRepository repository(
            AtomicReference<List<TrackHistoryEntity>> saved,
            AtomicBoolean failOnce) {
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
                                return Flux.defer(
                                        () -> {
                                            if (failOnce.getAndSet(false)) {
                                                return Flux.error(
                                                        new IllegalStateException(
                                                                "database unavailable"));
                                            }
                                            saved.set(
                                                    java.util.stream.Stream.concat(
                                                                    saved.get().stream(),
                                                                    rows.stream())
                                                            .toList());
                                            return Flux.fromIterable(rows);
                                        });
                            }
                            if ("findByTrackIdOrderByStateAtAsc".equals(
                                    method.getName())) {
                                long trackId = (long) arguments[0];
                                return Flux.fromIterable(saved.get())
                                        .filter(row -> row.trackId() == trackId);
                            }
                            if ("findMaxTrackId".equals(method.getName())) {
                                return Mono.just(
                                        saved.get().stream()
                                                .mapToLong(TrackHistoryEntity::trackId)
                                                .max()
                                                .orElse(0));
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static TrackService service(TrackHistoryRepository repository) {
        return service(repository, new SimpleMeterRegistry());
    }

    private static TrackService service(
            TrackHistoryRepository repository, SimpleMeterRegistry meters) {
        LocalTangentPlane plane = new LocalTangentPlane(43.65, -79.38, 0);
        return new TrackService(
                new FusionEngine(
                        plane,
                        new FusionConfig(1, 1, 3, 9.21, 1)),
                plane,
                repository,
                processedRepository(),
                transactions(),
                new ObjectMapper().findAndRegisterModules(),
                meters);
    }

    @SuppressWarnings("unchecked")
    private static ProcessedKafkaRecordRepository processedRepository() {
        AtomicReference<List<ProcessedKafkaRecordEntity>> saved =
                new AtomicReference<>(List.of());
        return (ProcessedKafkaRecordRepository)
                Proxy.newProxyInstance(
                        ProcessedKafkaRecordRepository.class.getClassLoader(),
                        new Class<?>[] {ProcessedKafkaRecordRepository.class},
                        (proxy, method, arguments) -> {
                            if ("findAllById".equals(method.getName())) {
                                Iterable<String> ids = (Iterable<String>) arguments[0];
                                List<String> requested =
                                        StreamSupport.stream(ids.spliterator(), false)
                                                .toList();
                                return Flux.fromIterable(saved.get())
                                        .filter(row -> requested.contains(row.id()));
                            }
                            if ("findByOutcomeOrderByObservedAtAsc".equals(
                                    method.getName())) {
                                String outcome = (String) arguments[0];
                                return Flux.fromIterable(saved.get())
                                        .filter(row -> row.outcome().equals(outcome))
                                        .sort(
                                                java.util.Comparator.comparing(
                                                        ProcessedKafkaRecordEntity::observedAt));
                            }
                            if ("saveAll".equals(method.getName())) {
                                List<ProcessedKafkaRecordEntity> rows =
                                        Flux.fromIterable(
                                                        (Iterable<ProcessedKafkaRecordEntity>)
                                                                arguments[0])
                                                .collectList()
                                                .block();
                                return Flux.defer(
                                        () -> {
                                            List<String> rowIds =
                                                    rows.stream()
                                                            .map(
                                                                    ProcessedKafkaRecordEntity
                                                                            ::id)
                                                            .toList();
                                            saved.set(
                                                    java.util.stream.Stream.concat(
                                                                    saved.get().stream()
                                                                            .filter(
                                                                                    row ->
                                                                                            !rowIds
                                                                                                    .contains(
                                                                                                            row
                                                                                                                    .id())),
                                                                    rows.stream())
                                                            .toList());
                                            return Flux.fromIterable(rows);
                                        });
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static TransactionalOperator transactions() {
        return (TransactionalOperator)
                Proxy.newProxyInstance(
                        TransactionalOperator.class.getClassLoader(),
                        new Class<?>[] {TransactionalOperator.class},
                        (proxy, method, arguments) -> {
                            if ("transactional".equals(method.getName())) {
                                return arguments[0];
                            }
                            throw new UnsupportedOperationException(method.getName());
                        });
    }

    private static Detection detection(
            String sourceId, String sourceType, double latitude, double longitude) {
        return detection(sourceId, sourceType, NOW, latitude, longitude);
    }

    private static Detection detection(
            String sourceId,
            String sourceType,
            Instant observedAt,
            double latitude,
            double longitude) {
        return new Detection(
                sourceId,
                sourceType,
                observedAt,
                observedAt.plusMillis(20),
                latitude,
                longitude,
                100.0,
                10.0,
                90.0,
                10,
                Map.of());
    }
}
