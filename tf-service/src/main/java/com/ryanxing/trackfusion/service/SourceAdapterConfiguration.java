package com.ryanxing.trackfusion.service;

import com.ryanxing.trackfusion.adapters.AisStreamAdapter;
import com.ryanxing.trackfusion.adapters.FixtureSourceAdapter;
import com.ryanxing.trackfusion.adapters.OpenMeteoEnricher;
import com.ryanxing.trackfusion.adapters.OpenSkyAdapter;
import com.ryanxing.trackfusion.adapters.RadarUdpAdapter;
import com.ryanxing.trackfusion.adapters.ResilientSourceAdapter;
import com.ryanxing.trackfusion.adapters.SourceAdapter;
import com.ryanxing.trackfusion.adapters.TrackEnricher;
import com.ryanxing.trackfusion.common.Detection;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.net.URI;
import java.net.http.HttpClient;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SourceAdapterConfiguration {
    private static final Logger LOG =
            LoggerFactory.getLogger(SourceAdapterConfiguration.class);

    @Bean
    HttpClient sourceHttpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.fixture.enabled",
            havingValue = "true",
            matchIfMissing = true)
    SourceAdapter fixtureAdsb(Clock clock) {
        Instant now = clock.instant();
        return new FixtureSourceAdapter(
                "opensky-fixture",
                "ADSB",
                List.of(detection("opensky-fixture", "ADSB", now, 43.65, -79.38, 10)));
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.fixture.enabled",
            havingValue = "true",
            matchIfMissing = true)
    SourceAdapter fixtureRadar(Clock clock) {
        Instant now = clock.instant();
        return new FixtureSourceAdapter(
                "radar-fixture",
                "RADAR",
                List.of(
                        detection(
                                "radar-fixture",
                                "RADAR",
                                now,
                                43.65,
                                -79.38,
                                50)));
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.fixture.enabled",
            havingValue = "true",
            matchIfMissing = true)
    SourceAdapter fixtureAis(Clock clock) {
        Instant now = clock.instant();
        return new FixtureSourceAdapter(
                "ais-fixture",
                "AIS",
                List.of(detection("ais-fixture", "AIS", now, 43.70, -79.30, 25)));
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.opensky.enabled",
            havingValue = "true")
    SourceAdapter openSky(
            HttpClient client,
            @Value("${track-fusion.sources.opensky.endpoint}") URI endpoint,
            @Value("${track-fusion.sources.opensky.bearer-token:}") String token,
            @Value("${track-fusion.sources.opensky.poll-interval:10s}")
                    Duration pollInterval,
            SourceHealthRegistry health) {
        return resilient(
                new OpenSkyAdapter(
                        "opensky", client, endpoint, 12, token),
                pollInterval,
                Duration.ofMinutes(5),
                pollInterval,
                health);
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.ais.enabled",
            havingValue = "true")
    SourceAdapter ais(
            HttpClient client,
            @Value("${track-fusion.sources.ais.endpoint}") URI endpoint,
            @Value("${track-fusion.sources.ais.api-key}") String apiKey,
            @Value("${track-fusion.sources.ais.south:42}") double south,
            @Value("${track-fusion.sources.ais.west:-81}") double west,
            @Value("${track-fusion.sources.ais.north:45}") double north,
            @Value("${track-fusion.sources.ais.east:-77}") double east,
            SourceHealthRegistry health) {
        return resilient(
                new AisStreamAdapter(
                        "aisstream",
                        client,
                        endpoint,
                        apiKey,
                        List.of(
                                new AisStreamAdapter.BoundingBox(
                                        south, west, north, east)),
                        25),
                Duration.ofSeconds(1),
                Duration.ofMinutes(1),
                null,
                health);
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.radar.enabled",
            havingValue = "true")
    SourceAdapter radar(
            @Value("${track-fusion.sources.radar.source-id:radar-east}")
                    String sourceId,
            @Value("${track-fusion.sources.radar.port:5005}") int port,
            SourceHealthRegistry health) {
        return resilient(
                new RadarUdpAdapter(sourceId, port),
                Duration.ofSeconds(1),
                Duration.ofSeconds(30),
                null,
                health);
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.sources.weather.enabled",
            havingValue = "true")
    TrackEnricher weather(HttpClient client) {
        return new OpenMeteoEnricher(client, Duration.ofMinutes(10), 0.1);
    }

    private static SourceAdapter resilient(
            SourceAdapter delegate,
            Duration minimumBackoff,
            Duration maximumBackoff,
            Duration repeatDelay,
            SourceHealthRegistry health) {
        CircuitBreaker breaker = CircuitBreaker.ofDefaults(delegate.sourceId());
        breaker.getEventPublisher()
                .onStateTransition(
                        event -> {
                            health.circuitTransition(
                                    delegate.sourceId(), delegate.sourceType());
                            LOG.warn(
                                    "Source {} circuit transitioned to {}",
                                    delegate.sourceId(),
                                    event.getStateTransition().getToState());
                        });
        return new ResilientSourceAdapter(
                delegate,
                breaker,
                minimumBackoff,
                maximumBackoff,
                repeatDelay,
                error -> {
                    health.error(delegate.sourceId(), delegate.sourceType(), "adapter");
                    LOG.warn("Source {} retrying", delegate.sourceId(), error);
                });
    }

    private static Detection detection(
            String sourceId,
            String sourceType,
            Instant now,
            double latitude,
            double longitude,
            double sigma) {
        return new Detection(
                sourceId,
                sourceType,
                now,
                now,
                latitude,
                longitude,
                sourceType.equals("AIS") ? null : 1_200.0,
                50.0,
                90.0,
                sigma,
                Map.of("fixture", "true"));
    }
}
