package com.ryanxing.trackfusion.service;

import com.ryanxing.trackfusion.common.Detection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "track-fusion.pipeline.enabled",
        havingValue = "true",
        matchIfMissing = true)
public final class DetectionBatchConsumer {
    private final TrackService tracks;
    private final AdapterKafkaPublisher sources;
    private final SourceHealthRegistry health;
    private final Clock clock;
    private final Duration tickInterval;
    private final Duration allowedLateness;

    public DetectionBatchConsumer(
            TrackService tracks,
            AdapterKafkaPublisher sources,
            SourceHealthRegistry health,
            Clock clock,
            @Value("${track-fusion.fusion.tick-interval:1s}")
                    Duration tickInterval,
            @Value("${track-fusion.fusion.allowed-lateness:2s}")
                    Duration allowedLateness) {
        if (tickInterval.isNegative()
                || tickInterval.isZero()
                || tickInterval.toMillis() == 0) {
            throw new IllegalArgumentException("tickInterval must be at least 1ms");
        }
        if (allowedLateness.isNegative()) {
            throw new IllegalArgumentException("allowedLateness cannot be negative");
        }
        this.tracks = tracks;
        this.sources = sources;
        this.health = health;
        this.clock = clock;
        this.tickInterval = tickInterval;
        this.allowedLateness = allowedLateness;
    }

    @KafkaListener(
            id = "${track-fusion.kafka.consumer-id:track-fusion}",
            topics = "${track-fusion.kafka.detections-topic:detections}",
            batch = "true")
    public synchronized void consume(
            List<ConsumerRecord<String, Detection>> records) {
        List<TrackService.KafkaDetection> staged =
                records.stream()
                        .map(
                                record -> {
                                    health.consumed(record.value());
                                    return new TrackService.KafkaDetection(
                                            record.topic(),
                                            record.partition(),
                                            record.offset(),
                                            record.value());
                                })
                        .toList();
        TrackService.StageResult result = tracks.stage(staged).block();
        staged.stream()
                .filter(record -> result.duplicateIds().contains(record.id()))
                .forEach(record -> health.redelivered(record.detection()));
    }

    @Scheduled(
            fixedDelayString = "${track-fusion.fusion.flush-interval:500ms}")
    public synchronized void flush() {
        boolean leader =
                Boolean.TRUE.equals(
                        tracks.claimLeadership().onErrorReturn(false).block());
        sources.setActive(leader);
        if (!leader) {
            return;
        }
        Instant watermark = clock.instant().minus(allowedLateness);
        TreeMap<Instant, List<TrackService.KafkaDetection>> ticks =
                new TreeMap<>();
        List<TrackService.KafkaDetection> pending =
                tracks.pendingRecords().collectList().block();
        pending.forEach(
                record ->
                        ticks.computeIfAbsent(
                                        tickFor(record.detection().observedAt()),
                                        ignored -> new ArrayList<>())
                                .add(record));
        while (!ticks.isEmpty() && !ticks.firstKey().isAfter(watermark)) {
            var tick = ticks.pollFirstEntry();
            TrackService.ProcessResult result =
                    tracks.process(tick.getKey(), tick.getValue()).block();
            tick.getValue().forEach(
                    record -> {
                        if (result.lateIds().contains(record.id())) {
                            health.late(record.detection());
                        }
                    });
        }
    }

    private Instant tickFor(Instant observedAt) {
        long intervalMillis = tickInterval.toMillis();
        long tickMillis =
                Math.floorDiv(observedAt.toEpochMilli(), intervalMillis)
                                * intervalMillis
                        + intervalMillis;
        return Instant.ofEpochMilli(tickMillis);
    }
}
