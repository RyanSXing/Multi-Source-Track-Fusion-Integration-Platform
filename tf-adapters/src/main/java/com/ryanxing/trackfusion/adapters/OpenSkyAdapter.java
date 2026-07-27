package com.ryanxing.trackfusion.adapters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.common.Detection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public final class OpenSkyAdapter implements SourceAdapter {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final String sourceId;
    private final HttpClient client;
    private final URI endpoint;
    private final Duration pollInterval;
    private final double positionSigmaMeters;
    private final String bearerToken;

    public OpenSkyAdapter(
            String sourceId,
            HttpClient client,
            URI endpoint,
            Duration pollInterval,
            double positionSigmaMeters,
            String bearerToken) {
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException("sourceId is required");
        }
        if (pollInterval == null || pollInterval.isNegative() || pollInterval.isZero()) {
            throw new IllegalArgumentException("pollInterval must be positive");
        }
        if (!Double.isFinite(positionSigmaMeters) || positionSigmaMeters <= 0) {
            throw new IllegalArgumentException("positionSigmaMeters must be positive");
        }
        this.sourceId = sourceId;
        this.client = Objects.requireNonNull(client, "client");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.pollInterval = pollInterval;
        this.positionSigmaMeters = positionSigmaMeters;
        this.bearerToken = bearerToken;
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public String sourceType() {
        return "ADSB";
    }

    @Override
    public Flux<Detection> stream() {
        return Flux.interval(Duration.ZERO, pollInterval)
                .concatMap(ignored -> fetch())
                .flatMapIterable(
                        json -> parse(sourceId, json, Instant.now(), positionSigmaMeters))
                .retryWhen(
                        Retry.backoff(Long.MAX_VALUE, pollInterval)
                                .maxBackoff(Duration.ofMinutes(5))
                                .transientErrors(true));
    }

    private Mono<String> fetch() {
        HttpRequest.Builder request = HttpRequest.newBuilder(endpoint).GET();
        if (bearerToken != null && !bearerToken.isBlank()) {
            request.header("Authorization", "Bearer " + bearerToken);
        }
        return Mono.fromFuture(client.sendAsync(request.build(), HttpResponse.BodyHandlers.ofString()))
                .flatMap(
                        response ->
                                response.statusCode() >= 200 && response.statusCode() < 300
                                        ? Mono.just(response.body())
                                        : Mono.error(
                                                new IllegalStateException(
                                                        "OpenSky HTTP "
                                                                + response.statusCode())));
    }

    static List<Detection> parse(
            String sourceId, String json, Instant receivedAt, double positionSigmaMeters) {
        Objects.requireNonNull(receivedAt, "receivedAt");
        try {
            JsonNode states = JSON.readTree(json).path("states");
            if (!states.isArray()) {
                return List.of();
            }
            List<Detection> detections = new ArrayList<>();
            for (JsonNode state : states) {
                if (!state.isArray()
                        || state.size() < 14
                        || !state.get(5).isNumber()
                        || !state.get(6).isNumber()) {
                    continue;
                }
                Map<String, String> attributes = new LinkedHashMap<>();
                putText(attributes, "icao24", state.get(0));
                putText(attributes, "callsign", state.get(1));
                putText(attributes, "originCountry", state.get(2));
                Instant observedAt =
                        epochSeconds(state.get(3), epochSeconds(state.get(4), receivedAt));
                Double altitude =
                        number(state.get(13)) != null
                                ? number(state.get(13))
                                : number(state.get(7));
                detections.add(
                        new Detection(
                                sourceId,
                                "ADSB",
                                observedAt,
                                receivedAt,
                                state.get(6).doubleValue(),
                                state.get(5).doubleValue(),
                                altitude,
                                nonNegative(number(state.get(9))),
                                heading(number(state.get(10))),
                                positionSigmaMeters,
                                attributes));
            }
            return List.copyOf(detections);
        } catch (JsonProcessingException invalidJson) {
            throw new IllegalArgumentException("invalid OpenSky JSON", invalidJson);
        }
    }

    private static Double number(JsonNode node) {
        return node != null && node.isNumber() && Double.isFinite(node.doubleValue())
                ? node.doubleValue()
                : null;
    }

    private static Double nonNegative(Double value) {
        return value != null && value >= 0 ? value : null;
    }

    private static Double heading(Double value) {
        if (value == null || value < 0 || value > 360) {
            return null;
        }
        return value == 360 ? 0 : value;
    }

    private static Instant epochSeconds(JsonNode node, Instant fallback) {
        return node != null && node.canConvertToLong()
                ? Instant.ofEpochSecond(node.longValue())
                : fallback;
    }

    private static void putText(Map<String, String> attributes, String key, JsonNode node) {
        if (node != null && !node.isNull()) {
            String value = node.asText().trim();
            if (!value.isEmpty()) {
                attributes.put(key, value);
            }
        }
    }
}
