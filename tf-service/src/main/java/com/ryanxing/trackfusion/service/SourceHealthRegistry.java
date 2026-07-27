package com.ryanxing.trackfusion.service;

import com.ryanxing.trackfusion.common.Detection;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public final class SourceHealthRegistry {
    private final Clock clock;
    private final MeterRegistry meters;
    private final ConcurrentHashMap<String, State> sources = new ConcurrentHashMap<>();

    public SourceHealthRegistry(Clock clock, MeterRegistry meters) {
        this.clock = clock;
        this.meters = meters;
    }

    public void message(Detection detection) {
        State state = state(detection.sourceId(), detection.sourceType());
        Instant now = clock.instant();
        state.messages.increment();
        state.lastMessageAt = now;
        state.ingest.increment();
        Duration delay = Duration.between(detection.receivedAt(), now);
        if (!delay.isNegative()) {
            state.latency.record(delay);
        }
    }

    public void error(String sourceId, String sourceType) {
        State state = state(sourceId, sourceType);
        state.errors.increment();
        state.failures.increment();
    }

    public List<SourceHealth> snapshots() {
        Instant now = clock.instant();
        return sources.values().stream()
                .map(state -> state.snapshot(now))
                .sorted(
                        Comparator.comparing(SourceHealth::sourceType)
                                .thenComparing(SourceHealth::sourceId))
                .toList();
    }

    private State state(String sourceId, String sourceType) {
        return sources.computeIfAbsent(
                sourceType + '\0' + sourceId,
                ignored -> new State(sourceId, sourceType, clock.instant(), meters));
    }

    public record SourceHealth(
            String sourceId,
            String sourceType,
            Instant lastMessageAt,
            long messageCount,
            long errorCount,
            double messageRatePerSecond) {}

    private static final class State {
        private final String sourceId;
        private final String sourceType;
        private final Instant startedAt;
        private final LongAdder messages = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final Counter ingest;
        private final Counter failures;
        private final Timer latency;
        private volatile Instant lastMessageAt;

        private State(
                String sourceId,
                String sourceType,
                Instant startedAt,
                MeterRegistry meters) {
            this.sourceId = sourceId;
            this.sourceType = sourceType;
            this.startedAt = startedAt;
            ingest =
                    meters.counter(
                            "track_fusion_source_ingest_total",
                            "source_id",
                            sourceId,
                            "source_type",
                            sourceType);
            failures =
                    meters.counter(
                            "track_fusion_source_errors_total",
                            "source_id",
                            sourceId,
                            "source_type",
                            sourceType);
            latency =
                    Timer.builder("track_fusion_source_latency")
                            .tags("source_id", sourceId, "source_type", sourceType)
                            .publishPercentileHistogram()
                            .register(meters);
        }

        private SourceHealth snapshot(Instant now) {
            long count = messages.sum();
            double seconds =
                    Math.max(
                            1,
                            Duration.between(startedAt, now).toNanos()
                                    / 1_000_000_000.0);
            return new SourceHealth(
                    sourceId,
                    sourceType,
                    lastMessageAt,
                    count,
                    errors.sum(),
                    count / seconds);
        }
    }
}
