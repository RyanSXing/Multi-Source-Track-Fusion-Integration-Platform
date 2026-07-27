package com.ryanxing.trackfusion.service;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;

@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@SpringBootTest(
        properties = {
            "track-fusion.fusion.allowed-lateness=0s",
            "track-fusion.fusion.flush-interval=50ms",
            "spring.kafka.consumer.auto-offset-reset=earliest"
        })
class TrackPipelineIntegrationTest {
    @Container
    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:17-alpine")
                    .withDatabaseName("trackfusion")
                    .withUsername("trackfusion")
                    .withPassword("trackfusion");

    @Container
    static final RedpandaContainer REDPANDA =
            new RedpandaContainer(
                    DockerImageName.parse(
                            "docker.redpanda.com/redpandadata/redpanda:v25.1.9"));

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry properties) {
        properties.add(
                "spring.r2dbc.url",
                () ->
                        "r2dbc:postgresql://"
                                + POSTGRES.getHost()
                                + ':'
                                + POSTGRES.getMappedPort(5432)
                                + "/trackfusion");
        properties.add("spring.r2dbc.username", POSTGRES::getUsername);
        properties.add("spring.r2dbc.password", POSTGRES::getPassword);
        properties.add(
                "spring.kafka.bootstrap-servers",
                REDPANDA::getBootstrapServers);
    }

    @Autowired private TrackService tracks;
    @Autowired private TrackHistoryRepository history;
    @Autowired private ProcessedKafkaRecordRepository records;

    @Test
    void movesFixtureAdaptersThroughRedpandaIntoPersistedFusedTracks() {
        List<TrackView> result =
                Flux.interval(Duration.ofMillis(100))
                        .map(ignored -> tracks.currentTracks())
                        .filter(values -> values.size() == 2)
                        .next()
                        .block(Duration.ofSeconds(30));

        assertThat(result).isNotNull();
        assertThat(result)
                .anySatisfy(
                        track ->
                                assertThat(
                                                track.contributors().stream()
                                                        .map(
                                                                detection ->
                                                                        detection
                                                                                .sourceType())
                                                        .collect(
                                                                java.util.stream
                                                                        .Collectors
                                                                        .toSet()))
                                        .isEqualTo(Set.of("ADSB", "RADAR")))
                .anySatisfy(
                        track ->
                                assertThat(
                                                track.contributors().stream()
                                                        .map(
                                                                detection ->
                                                                        detection
                                                                                .sourceType())
                                                        .toList())
                                        .containsExactly("AIS"));
        assertThat(history.count().block()).isEqualTo(2);
        assertThat(records.count().block()).isEqualTo(3);
        assertThat(
                        history.findAll()
                                .map(TrackHistoryEntity::status)
                                .collectList()
                                .block())
                .containsOnly("TENTATIVE");
    }
}
