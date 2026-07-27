package com.ryanxing.trackfusion.service;

import com.ryanxing.trackfusion.adapters.SourceAdapter;
import com.ryanxing.trackfusion.adapters.TrackEnricher;
import com.ryanxing.trackfusion.common.Detection;
import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@ConditionalOnProperty(
        name = "track-fusion.pipeline.enabled",
        havingValue = "true",
        matchIfMissing = true)
public final class AdapterKafkaPublisher {
    private static final Logger LOG =
            LoggerFactory.getLogger(AdapterKafkaPublisher.class);

    private final List<SourceAdapter> adapters;
    private final List<TrackEnricher> enrichers;
    private final KafkaTemplate<String, Detection> kafka;
    private final SourceHealthRegistry health;
    private final String topic;
    private Disposable.Composite subscriptions = Disposables.composite();
    private boolean active;

    public AdapterKafkaPublisher(
            List<SourceAdapter> adapters,
            List<TrackEnricher> enrichers,
            KafkaTemplate<String, Detection> kafka,
            SourceHealthRegistry health,
            @Value("${track-fusion.kafka.detections-topic:detections}") String topic) {
        this.adapters = adapters;
        this.enrichers = enrichers;
        this.kafka = kafka;
        this.health = health;
        this.topic = topic;
    }

    synchronized void setActive(boolean shouldBeActive) {
        if (active == shouldBeActive) {
            return;
        }
        active = shouldBeActive;
        if (!active) {
            subscriptions.dispose();
            subscriptions = Disposables.composite();
            return;
        }
        adapters.forEach(
                adapter ->
                        subscriptions.add(
                                adapter.stream()
                                        .concatMap(this::publish)
                                        .subscribe(
                                                ignored -> {},
                                                error -> {
                                                    health.error(
                                                            adapter.sourceId(),
                                                            adapter.sourceType(),
                                                            "adapter");
                                                    LOG.error(
                                                            "Source {} failed",
                                                            adapter.sourceId(),
                                                            error);
                                                })));
    }

    Mono<Void> publish(Detection detection) {
        health.adapterReceived(detection);
        Mono<Detection> enriched = Mono.just(detection);
        for (TrackEnricher enricher : enrichers) {
            enriched =
                    enriched.flatMap(
                            current ->
                                    enricher.enrich(current)
                                            .doOnSuccess(
                                                    ignored ->
                                                            health.recovered(
                                                                    enricher.sourceType(),
                                                                    enricher.sourceType(),
                                                                    "enrichment"))
                                            .onErrorResume(
                                                    error -> {
                                                        health.error(
                                                                enricher.sourceType(),
                                                                enricher.sourceType(),
                                                                "enrichment");
                                                        LOG.warn(
                                                                "{} enrichment failed",
                                                                enricher.sourceType(),
                                                                error);
                                                        return Mono.just(current);
                                                    }));
        }
        return enriched.flatMap(
                current ->
                        Mono.defer(
                                        () ->
                                                Mono.fromFuture(
                                                        kafka.send(
                                                                topic,
                                                                current.sourceType()
                                                                        + ':'
                                                                        + current.sourceId(),
                                                                current)))
                                .doOnSuccess(ignored -> health.published(current))
                                .retryWhen(
                                        Retry.backoff(Long.MAX_VALUE, Duration.ofSeconds(1))
                                                .maxBackoff(Duration.ofSeconds(30))
                                                .transientErrors(true)
                                                .doBeforeRetry(
                                                        retry -> {
                                                            health.error(
                                                                    current.sourceId(),
                                                                    current.sourceType(),
                                                                    "publish");
                                                            LOG.warn(
                                                                    "Kafka publish failed for {}",
                                                                    current.sourceId(),
                                                                    retry.failure());
                                                        }))
                                .then());
    }

    @PreDestroy
    public synchronized void stop() {
        setActive(false);
    }
}
