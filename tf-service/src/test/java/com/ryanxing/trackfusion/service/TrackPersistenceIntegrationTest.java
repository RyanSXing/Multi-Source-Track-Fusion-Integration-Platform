package com.ryanxing.trackfusion.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.common.Detection;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.fusion.FusionConfig;
import com.ryanxing.trackfusion.fusion.FusionEngine;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.r2dbc.spi.ConnectionFactory;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.transaction.reactive.TransactionalOperator;

@SpringBootTest(
        properties = {
            "track-fusion.pipeline.enabled=false",
            "spring.r2dbc.url=r2dbc:tc:postgresql:///trackfusion?TC_IMAGE_TAG=17-alpine",
            "spring.r2dbc.username=test",
            "spring.r2dbc.password=test",
            "spring.sql.init.mode=always"
        })
class TrackPersistenceIntegrationTest {
    private static final Instant OBSERVED =
            Instant.parse("2026-01-02T03:04:05Z");

    @Autowired private TrackService tracks;
    @Autowired private TrackHistoryRepository history;
    @Autowired private ProcessedKafkaRecordRepository records;
    @Autowired private TransactionalOperator transactions;
    @Autowired private ObjectMapper json;
    @Autowired private ConnectionFactory connections;
    @Autowired private DatabaseClient database;

    @BeforeEach
    void clearDatabase() {
        history.deleteAll().then(records.deleteAll()).block();
    }

    @Test
    void fencesADeadLeaderBeforeStandbyTakeover() {
        List<TrackService.KafkaDetection> batch =
                List.of(
                        kafka(0, "opensky", "ADSB"),
                        kafka(1, "radar-east", "RADAR"));
        LocalTangentPlane plane = new LocalTangentPlane(43.65, -79.38, 0);
        TrackService standby =
                new TrackService(
                        new FusionEngine(plane, FusionConfig.defaults()),
                        plane,
                        history,
                        records,
                        transactions,
                        json,
                        new SimpleMeterRegistry(),
                        connections);
        Clock clock =
                Clock.fixed(OBSERVED.plusSeconds(10), ZoneOffset.UTC);
        DetectionBatchConsumer standbyConsumer =
                consumer(standby, clock);

        try {
            assertThat(tracks.claimLeadership().block()).isTrue();
            assertThat(standby.claimLeadership().block()).isFalse();
            tracks.stage(batch).block();

            standbyConsumer.flush();
            assertThat(history.count().block()).isZero();
            Integer leaderPid =
                    database.sql(
                                    """
                                    SELECT pid
                                    FROM pg_locks
                                    WHERE locktype = 'advisory' AND granted
                                    LIMIT 1
                                    """)
                            .map((row, metadata) -> row.get("pid", Integer.class))
                            .one()
                            .block();
            assertThat(
                            database.sql(
                                            "SELECT pg_terminate_backend(:pid) AS terminated")
                                    .bind("pid", leaderPid)
                                    .map(
                                            (row, metadata) ->
                                                    row.get(
                                                            "terminated",
                                                            Boolean.class))
                                    .one()
                                    .block())
                    .isTrue();
            assertThat(standby.claimLeadership().block()).isTrue();
            assertThatThrownBy(
                            () ->
                                    tracks.process(
                                                    OBSERVED.plusSeconds(1),
                                                    batch)
                                            .block())
                    .hasMessageContaining(
                            "Fusion leadership changed before commit");
            standbyConsumer.flush();

            assertThat(tracks.claimLeadership().block()).isFalse();
            assertThat(history.count().block()).isEqualTo(1);
            assertThat(
                            records.findAll()
                                    .map(ProcessedKafkaRecordEntity::outcome)
                                    .collectList()
                                    .block())
                    .containsOnly("FUSED");
        } finally {
            standby.close();
        }
    }

    private static DetectionBatchConsumer consumer(
            TrackService service, Clock clock) {
        return new DetectionBatchConsumer(
                service,
                new AdapterKafkaPublisher(
                        List.of(), List.of(), null,
                        new SourceHealthRegistry(clock, new SimpleMeterRegistry()),
                        "detections"),
                new SourceHealthRegistry(clock, new SimpleMeterRegistry()),
                clock,
                Duration.ofSeconds(1),
                Duration.ZERO);
    }

    private static TrackService.KafkaDetection kafka(
            long offset, String sourceId, String sourceType) {
        Detection detection =
                new Detection(
                        sourceId,
                        sourceType,
                        OBSERVED,
                        OBSERVED.plusMillis(20),
                        43.65,
                        -79.38,
                        100.0,
                        10.0,
                        90.0,
                        10,
                        Map.of());
        return new TrackService.KafkaDetection(
                "detections", 0, offset, detection);
    }
}
