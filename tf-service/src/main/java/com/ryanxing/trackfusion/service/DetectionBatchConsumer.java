package com.ryanxing.trackfusion.service;

import com.ryanxing.trackfusion.common.Detection;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "track-fusion.pipeline.enabled",
        havingValue = "true",
        matchIfMissing = true)
public final class DetectionBatchConsumer {
    private final TrackService tracks;
    private final SourceHealthRegistry health;
    private final Clock clock;
    private Instant lastTick;

    public DetectionBatchConsumer(
            TrackService tracks, SourceHealthRegistry health, Clock clock) {
        this.tracks = tracks;
        this.health = health;
        this.clock = clock;
    }

    @KafkaListener(
            id = "${track-fusion.kafka.consumer-id:track-fusion}",
            topics = "${track-fusion.kafka.detections-topic:detections}",
            batch = "true")
    public void consume(List<Detection> detections) {
        if (detections.isEmpty()) {
            return;
        }
        detections.forEach(health::message);
        tracks.process(nextTick(detections), detections).block();
    }

    private synchronized Instant nextTick(List<Detection> detections) {
        Instant tick =
                detections.stream()
                        .map(Detection::observedAt)
                        .max(Instant::compareTo)
                        .orElseThrow();
        Instant now = clock.instant();
        if (now.isAfter(tick)) {
            tick = now;
        }
        if (lastTick != null && !tick.isAfter(lastTick)) {
            tick = lastTick.plusNanos(1);
        }
        lastTick = tick;
        return tick;
    }
}
