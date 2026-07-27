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
import java.util.Set;
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

    public void adapterReceived(Detection detection) {
        State state = state(detection.sourceId(), detection.sourceType());
        state.adapterReceived.increment();
        state.failedStages.remove("adapter");
        state.adapterReceivedMetric.increment();
    }

    public void published(Detection detection) {
        State state = state(detection.sourceId(), detection.sourceType());
        state.kafkaPublished.increment();
        state.failedStages.remove("publish");
        state.kafkaPublishedMetric.increment();
    }

    public void consumed(Detection detection) {
        State state = state(detection.sourceId(), detection.sourceType());
        Instant now = clock.instant();
        state.kafkaConsumed.increment();
        state.lastMessageAt = now;
        state.kafkaConsumedMetric.increment();
        Duration delay = Duration.between(detection.receivedAt(), now);
        if (!delay.isNegative()) {
            state.latency.record(delay);
        }
    }

    public void redelivered(Detection detection) {
        State state = state(detection.sourceId(), detection.sourceType());
        state.redelivered.increment();
        state.redeliveredMetric.increment();
    }

    public void late(Detection detection) {
        State state = state(detection.sourceId(), detection.sourceType());
        state.late.increment();
        state.lateMetric.increment();
    }

    public void error(String sourceId, String sourceType, String stage) {
        State state = state(sourceId, sourceType);
        state.errors.increment();
        state.failedStages.add(stage);
        state.failures.increment();
    }

    public void recovered(String sourceId, String sourceType, String stage) {
        State state = sources.get(sourceType + '\0' + sourceId);
        if (state != null) {
            state.failedStages.remove(stage);
        }
    }

    public void circuitTransition(String sourceId, String sourceType) {
        State state = state(sourceId, sourceType);
        state.circuitTransitions.increment();
        state.circuitTransitionMetric.increment();
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
            long adapterReceivedCount,
            long kafkaPublishedCount,
            long kafkaConsumedCount,
            long redeliveredCount,
            long lateCount,
            long errorCount,
            long circuitTransitionCount,
            boolean degraded,
            double messageRatePerSecond) {}

    private static final class State {
        private final String sourceId;
        private final String sourceType;
        private final Instant startedAt;
        private final LongAdder adapterReceived = new LongAdder();
        private final LongAdder kafkaPublished = new LongAdder();
        private final LongAdder kafkaConsumed = new LongAdder();
        private final LongAdder redelivered = new LongAdder();
        private final LongAdder late = new LongAdder();
        private final LongAdder errors = new LongAdder();
        private final LongAdder circuitTransitions = new LongAdder();
        private final Set<String> failedStages = ConcurrentHashMap.newKeySet();
        private final Counter adapterReceivedMetric;
        private final Counter kafkaPublishedMetric;
        private final Counter kafkaConsumedMetric;
        private final Counter redeliveredMetric;
        private final Counter lateMetric;
        private final Counter failures;
        private final Counter circuitTransitionMetric;
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
            adapterReceivedMetric = counter(meters, "adapter_received");
            kafkaPublishedMetric = counter(meters, "kafka_published");
            kafkaConsumedMetric = counter(meters, "kafka_consumed");
            redeliveredMetric = counter(meters, "redelivered");
            lateMetric = counter(meters, "late");
            failures = counter(meters, "errors");
            circuitTransitionMetric = counter(meters, "circuit_transitions");
            latency =
                    Timer.builder("track_fusion_source_latency")
                            .tags("source_id", sourceId, "source_type", sourceType)
                            .publishPercentileHistogram()
                            .register(meters);
        }

        private Counter counter(MeterRegistry meters, String stage) {
            return meters.counter(
                    "track_fusion_source_events_total",
                    "source_id",
                    sourceId,
                    "source_type",
                    sourceType,
                    "stage",
                    stage);
        }

        private SourceHealth snapshot(Instant now) {
            long count = kafkaConsumed.sum();
            double seconds =
                    Math.max(
                            1,
                            Duration.between(startedAt, now).toNanos()
                                    / 1_000_000_000.0);
            return new SourceHealth(
                    sourceId,
                    sourceType,
                    lastMessageAt,
                    adapterReceived.sum(),
                    kafkaPublished.sum(),
                    count,
                    redelivered.sum(),
                    late.sum(),
                    errors.sum(),
                    circuitTransitions.sum(),
                    !failedStages.isEmpty(),
                    count / seconds);
        }
    }
}
