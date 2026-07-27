package com.ryanxing.trackfusion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class TrackWebSocketHandler implements WebSocketHandler {
    private final TrackService tracks;
    private final ObjectMapper json;

    public TrackWebSocketHandler(TrackService tracks, ObjectMapper json) {
        this.tracks = tracks;
        this.json = json;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        return session.send(
                events(Duration.ofSeconds(10))
                        .map(this::encode)
                        .map(session::textMessage));
    }

    Flux<TrackService.TrackEvent> events(Duration snapshotInterval) {
        return Flux.defer(
                        () -> {
                            TrackService.TrackEvent initial = tracks.snapshot();
                            return Flux.concat(
                                    Mono.just(initial),
                                    Flux.merge(
                                            tracks.updates()
                                                    .filter(
                                                            event ->
                                                                    event.version()
                                                                            > initial.version()),
                                            Flux.interval(snapshotInterval)
                                                    .map(ignored -> tracks.snapshot())));
                        })
                .onBackpressureLatest();
    }

    private String encode(TrackService.TrackEvent event) {
        try {
            return json.writeValueAsString(event);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize track update", error);
        }
    }
}
