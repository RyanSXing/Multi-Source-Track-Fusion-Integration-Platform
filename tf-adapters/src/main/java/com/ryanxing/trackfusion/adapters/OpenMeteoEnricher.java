package com.ryanxing.trackfusion.adapters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.common.Detection;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import reactor.core.publisher.Mono;

public final class OpenMeteoEnricher implements TrackEnricher {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Function<URI, CompletableFuture<String>> fetcher;
    private final Clock clock;
    private final Duration timeToLive;
    private final double gridDegrees;
    // ponytail: process-local cache; add bounded eviction if worldwide cell count grows.
    private final Map<GridCell, CacheEntry> cache = new HashMap<>();

    public OpenMeteoEnricher(
            HttpClient client, Duration timeToLive, double gridDegrees) {
        this(
                uri -> fetch(Objects.requireNonNull(client, "client"), uri),
                Clock.systemUTC(),
                timeToLive,
                gridDegrees);
    }

    OpenMeteoEnricher(
            Function<URI, CompletableFuture<String>> fetcher,
            Clock clock,
            Duration timeToLive,
            double gridDegrees) {
        if (timeToLive == null || timeToLive.isNegative() || timeToLive.isZero()) {
            throw new IllegalArgumentException("timeToLive must be positive");
        }
        if (!Double.isFinite(gridDegrees) || gridDegrees <= 0 || gridDegrees > 1) {
            throw new IllegalArgumentException("gridDegrees must be in (0, 1]");
        }
        this.fetcher = Objects.requireNonNull(fetcher, "fetcher");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.timeToLive = timeToLive;
        this.gridDegrees = gridDegrees;
    }

    @Override
    public String sourceType() {
        return "WEATHER";
    }

    @Override
    public Mono<Detection> enrich(Detection detection) {
        Objects.requireNonNull(detection, "detection");
        GridCell cell = GridCell.from(detection.latDeg(), detection.lonDeg(), gridDegrees);
        return Mono.fromFuture(weather(cell))
                .map(attributes -> withAttributes(detection, attributes));
    }

    private synchronized CompletableFuture<Map<String, String>> weather(GridCell cell) {
        Instant now = clock.instant();
        CacheEntry cached = cache.get(cell);
        if (cached != null && cached.expiresAt.isAfter(now)) {
            return cached.weather;
        }

        CompletableFuture<Map<String, String>> future =
                fetcher.apply(uri(cell)).thenApply(OpenMeteoEnricher::parse);
        CacheEntry replacement = new CacheEntry(now.plus(timeToLive), future);
        cache.put(cell, replacement);
        future.whenComplete(
                (ignored, error) -> {
                    if (error != null) {
                        synchronized (this) {
                            cache.remove(cell, replacement);
                        }
                    }
                });
        return future;
    }

    private URI uri(GridCell cell) {
        return URI.create(
                "https://api.open-meteo.com/v1/forecast?latitude="
                        + cell.latitude(gridDegrees)
                        + "&longitude="
                        + cell.longitude(gridDegrees)
                        + "&current=temperature_2m,wind_speed_10m,wind_direction_10m,weather_code");
    }

    private static CompletableFuture<String> fetch(HttpClient client, URI uri) {
        HttpRequest request = HttpRequest.newBuilder(uri).GET().build();
        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
                .thenApply(
                        response -> {
                            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                                throw new IllegalStateException(
                                        "Open-Meteo HTTP " + response.statusCode());
                            }
                            return response.body();
                        });
    }

    private static Map<String, String> parse(String json) {
        try {
            JsonNode current = JSON.readTree(json).path("current");
            if (!current.isObject()) {
                throw new IllegalArgumentException("Open-Meteo response has no current data");
            }
            Map<String, String> attributes = new LinkedHashMap<>();
            putDouble(attributes, "weather.temperatureC", current.path("temperature_2m"), 1);
            putDouble(attributes, "weather.windSpeedMps", current.path("wind_speed_10m"), 1 / 3.6);
            putDouble(
                    attributes,
                    "weather.windDirectionDeg",
                    current.path("wind_direction_10m"),
                    1);
            if (current.path("weather_code").isIntegralNumber()) {
                attributes.put(
                        "weather.code", Integer.toString(current.path("weather_code").asInt()));
            }
            return Map.copyOf(attributes);
        } catch (JsonProcessingException invalidJson) {
            throw new IllegalArgumentException("invalid Open-Meteo JSON", invalidJson);
        }
    }

    private static void putDouble(
            Map<String, String> attributes, String key, JsonNode value, double multiplier) {
        if (value.isNumber() && Double.isFinite(value.doubleValue())) {
            attributes.put(key, Double.toString(value.doubleValue() * multiplier));
        }
    }

    private static Detection withAttributes(
            Detection detection, Map<String, String> additional) {
        Map<String, String> attributes = new LinkedHashMap<>(detection.attributes());
        attributes.putAll(additional);
        return new Detection(
                detection.sourceId(),
                detection.sourceType(),
                detection.observedAt(),
                detection.receivedAt(),
                detection.latDeg(),
                detection.lonDeg(),
                detection.altMeters(),
                detection.speedMps(),
                detection.headingDeg(),
                detection.positionSigmaMeters(),
                attributes);
    }

    private record GridCell(long latitudeIndex, long longitudeIndex) {
        private static GridCell from(double latitude, double longitude, double gridDegrees) {
            return new GridCell(
                    roundedIndex(latitude, gridDegrees),
                    roundedIndex(longitude, gridDegrees));
        }

        private static long roundedIndex(double value, double gridDegrees) {
            return BigDecimal.valueOf(value)
                    .divide(BigDecimal.valueOf(gridDegrees), 0, RoundingMode.HALF_UP)
                    .longValueExact();
        }

        private double latitude(double gridDegrees) {
            return latitudeIndex * gridDegrees;
        }

        private double longitude(double gridDegrees) {
            return longitudeIndex * gridDegrees;
        }
    }

    private record CacheEntry(
            Instant expiresAt, CompletableFuture<Map<String, String>> weather) {}
}
