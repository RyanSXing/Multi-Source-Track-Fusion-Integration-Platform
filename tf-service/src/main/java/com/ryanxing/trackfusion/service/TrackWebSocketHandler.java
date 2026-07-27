package com.ryanxing.trackfusion.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketSession;
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
                tracks.updates()
                        .map(this::encode)
                        .map(session::textMessage));
    }

    private String encode(TrackView track) {
        try {
            return json.writeValueAsString(track);
        } catch (JsonProcessingException error) {
            throw new IllegalStateException("Could not serialize track update", error);
        }
    }
}
