package com.ryanxing.trackfusion.adapters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ryanxing.trackfusion.common.Detection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public final class AisStreamAdapter implements SourceAdapter {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final double KNOTS_TO_METERS_PER_SECOND = 0.514_444;

    private final String sourceId;
    private final Supplier<Flux<String>> messages;
    private final double positionSigmaMeters;

    public AisStreamAdapter(
            String sourceId,
            HttpClient client,
            URI endpoint,
            String apiKey,
            List<BoundingBox> boundingBoxes,
            double positionSigmaMeters) {
        this(
                sourceId,
                liveMessages(client, endpoint, apiKey, boundingBoxes),
                positionSigmaMeters);
    }

    AisStreamAdapter(
            String sourceId,
            Supplier<Flux<String>> messages,
            double positionSigmaMeters) {
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException("sourceId is required");
        }
        if (!Double.isFinite(positionSigmaMeters) || positionSigmaMeters <= 0) {
            throw new IllegalArgumentException("positionSigmaMeters must be positive");
        }
        this.sourceId = sourceId;
        this.messages = Objects.requireNonNull(messages, "messages");
        this.positionSigmaMeters = positionSigmaMeters;
    }

    @Override
    public String sourceId() {
        return sourceId;
    }

    @Override
    public String sourceType() {
        return "AIS";
    }

    @Override
    public Flux<Detection> stream() {
        return Flux.defer(messages)
                .map(json -> parse(sourceId, json, Instant.now(), positionSigmaMeters))
                .filter(Optional::isPresent)
                .map(Optional::orElseThrow);
    }

    private static Supplier<Flux<String>> liveMessages(
            HttpClient client,
            URI endpoint,
            String apiKey,
            List<BoundingBox> boundingBoxes) {
        HttpClient requiredClient = Objects.requireNonNull(client, "client");
        URI requiredEndpoint = Objects.requireNonNull(endpoint, "endpoint");
        String encodedSubscription = subscription(apiKey, boundingBoxes);
        return () -> webSocketMessages(requiredClient, requiredEndpoint, encodedSubscription);
    }

    static Optional<Detection> parse(
            String sourceId, String json, Instant receivedAt, double positionSigmaMeters) {
        try {
            JsonNode root = JSON.readTree(json);
            if (!"PositionReport".equals(root.path("MessageType").asText())) {
                return Optional.empty();
            }
            JsonNode metadata =
                    root.has("MetaData") ? root.path("MetaData") : root.path("Metadata");
            JsonNode position = root.path("Message").path("PositionReport");
            Double latitude = number(position, "Latitude", metadata, "Latitude");
            Double longitude = number(position, "Longitude", metadata, "Longitude");
            if (latitude == null
                    || latitude < -90
                    || latitude > 90
                    || longitude == null
                    || longitude < -180
                    || longitude > 180) {
                return Optional.empty();
            }

            Map<String, String> attributes = new LinkedHashMap<>();
            putText(attributes, "mmsi", metadata.path("MMSI"));
            if (!attributes.containsKey("mmsi")) {
                putText(attributes, "mmsi", position.path("UserID"));
            }
            putText(attributes, "shipName", metadata.path("ShipName"));
            Instant observedAt =
                    instant(metadata.path("time_utc").asText(null), receivedAt);
            Double speedKnots = finite(position.path("Sog"));
            Double speed =
                    speedKnots != null && speedKnots >= 0 && speedKnots < 102.3
                            ? speedKnots * KNOTS_TO_METERS_PER_SECOND
                            : null;
            Double heading = heading(finite(position.path("TrueHeading")));
            if (heading == null) {
                heading = heading(finite(position.path("Cog")));
            }
            return Optional.of(
                    new Detection(
                            sourceId,
                            "AIS",
                            observedAt,
                            receivedAt,
                            latitude,
                            longitude,
                            null,
                            speed,
                            heading,
                            positionSigmaMeters,
                            attributes));
        } catch (JsonProcessingException invalidJson) {
            throw new IllegalArgumentException("invalid AIS JSON", invalidJson);
        }
    }

    private static Flux<String> webSocketMessages(
            HttpClient client, URI endpoint, String subscription) {
        return webSocketMessages(
                listener -> client.newWebSocketBuilder().buildAsync(endpoint, listener),
                subscription);
    }

    static Flux<String> webSocketMessages(
            Function<WebSocket.Listener, CompletableFuture<WebSocket>> connector,
            String subscription) {
        Objects.requireNonNull(connector, "connector");
        Objects.requireNonNull(subscription, "subscription");
        return Flux.create(
                sink -> {
                    AtomicReference<WebSocket> socket = new AtomicReference<>();
                    StringBuilder message = new StringBuilder();
                    WebSocket.Listener listener =
                            new WebSocket.Listener() {
                                @Override
                                public void onOpen(WebSocket webSocket) {
                                    socket.set(webSocket);
                                    if (sink.isCancelled()) {
                                        webSocket.abort();
                                        return;
                                    }
                                    try {
                                        webSocket
                                                .sendText(subscription, true)
                                                .whenComplete(
                                                        (ignored, error) -> {
                                                            if (error != null
                                                                    && !sink.isCancelled()) {
                                                                sink.error(error);
                                                            } else if (!sink.isCancelled()) {
                                                                webSocket.request(1);
                                                            }
                                                        });
                                    } catch (RuntimeException sendFailure) {
                                        sink.error(sendFailure);
                                    }
                                }

                                @Override
                                public CompletionStage<?> onText(
                                        WebSocket webSocket,
                                        CharSequence data,
                                        boolean last) {
                                    message.append(data);
                                    if (last) {
                                        sink.next(message.toString());
                                        message.setLength(0);
                                    }
                                    if (!sink.isCancelled()) {
                                        webSocket.request(1);
                                    }
                                    return CompletableFuture.completedFuture(null);
                                }

                                @Override
                                public CompletionStage<?> onClose(
                                        WebSocket webSocket,
                                        int statusCode,
                                        String reason) {
                                    if (!sink.isCancelled()) {
                                        sink.error(
                                                new IllegalStateException(
                                                        "AIS WebSocket closed: " + statusCode));
                                    }
                                    return CompletableFuture.completedFuture(null);
                                }

                                @Override
                                public void onError(WebSocket webSocket, Throwable error) {
                                    if (!sink.isCancelled()) {
                                        sink.error(error);
                                    }
                                }
                            };
                    CompletableFuture<WebSocket> connecting = connector.apply(listener);
                    connecting
                            .whenComplete(
                                    (webSocket, error) -> {
                                        if (error != null && !sink.isCancelled()) {
                                            sink.error(error);
                                        } else if (webSocket != null && sink.isCancelled()) {
                                            webSocket.abort();
                                        }
                                    });
                    sink.onDispose(
                            () -> {
                                connecting.cancel(true);
                                WebSocket webSocket = socket.get();
                                if (webSocket != null) {
                                    webSocket.abort();
                                }
                            });
                },
                FluxSink.OverflowStrategy.LATEST);
    }

    private static String subscription(
            String apiKey, List<BoundingBox> boundingBoxes) {
        if (apiKey == null || apiKey.isBlank()) {
            throw new IllegalArgumentException("AISStream API key is required");
        }
        if (boundingBoxes == null || boundingBoxes.isEmpty()) {
            throw new IllegalArgumentException("at least one bounding box is required");
        }
        ObjectNode root = JSON.createObjectNode().put("APIKey", apiKey);
        ArrayNode boxes = root.putArray("BoundingBoxes");
        for (BoundingBox box : boundingBoxes) {
            ArrayNode encoded = boxes.addArray();
            encoded.addArray().add(box.firstLatitude()).add(box.firstLongitude());
            encoded.addArray().add(box.secondLatitude()).add(box.secondLongitude());
        }
        root.putArray("FilterMessageTypes").add("PositionReport");
        return root.toString();
    }

    private static Double number(
            JsonNode primary, String primaryName, JsonNode fallback, String fallbackName) {
        Double value = finite(primary.path(primaryName));
        return value != null ? value : finite(fallback.path(fallbackName));
    }

    private static Double finite(JsonNode node) {
        return node != null && node.isNumber() && Double.isFinite(node.doubleValue())
                ? node.doubleValue()
                : null;
    }

    private static Double heading(Double value) {
        return value != null && value >= 0 && value < 360 ? value : null;
    }

    private static Instant instant(String value, Instant fallback) {
        try {
            return value == null ? fallback : Instant.parse(value);
        } catch (DateTimeParseException invalidTime) {
            return fallback;
        }
    }

    private static void putText(Map<String, String> attributes, String key, JsonNode node) {
        if (node != null && !node.isMissingNode() && !node.isNull()) {
            String value = node.asText().trim();
            if (!value.isEmpty()) {
                attributes.put(key, value);
            }
        }
    }

    public record BoundingBox(
            double firstLatitude,
            double firstLongitude,
            double secondLatitude,
            double secondLongitude) {
        public BoundingBox {
            if (!validLatitude(firstLatitude)
                    || !validLatitude(secondLatitude)
                    || !validLongitude(firstLongitude)
                    || !validLongitude(secondLongitude)) {
                throw new IllegalArgumentException("invalid AIS bounding box");
            }
        }

        private static boolean validLatitude(double value) {
            return Double.isFinite(value) && value >= -90 && value <= 90;
        }

        private static boolean validLongitude(double value) {
            return Double.isFinite(value) && value >= -180 && value <= 180;
        }
    }
}
