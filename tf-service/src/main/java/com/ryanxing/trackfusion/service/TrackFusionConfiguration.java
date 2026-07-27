package com.ryanxing.trackfusion.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryanxing.trackfusion.common.LocalTangentPlane;
import com.ryanxing.trackfusion.fusion.FusionConfig;
import com.ryanxing.trackfusion.fusion.FusionEngine;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.server.support.WebSocketHandlerAdapter;

@Configuration
public class TrackFusionConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    LocalTangentPlane localTangentPlane(
            @Value("${track-fusion.origin.latitude:43.65}") double latitude,
            @Value("${track-fusion.origin.longitude:-79.38}") double longitude,
            @Value("${track-fusion.origin.altitude-meters:0}") double altitudeMeters) {
        return new LocalTangentPlane(latitude, longitude, altitudeMeters);
    }

    @Bean
    FusionEngine fusionEngine(LocalTangentPlane plane) {
        return new FusionEngine(plane, FusionConfig.defaults());
    }

    @Bean
    SourceHealthRegistry sourceHealthRegistry(Clock clock, MeterRegistry meters) {
        return new SourceHealthRegistry(clock, meters);
    }

    @Bean
    TrackService trackService(
            FusionEngine engine,
            LocalTangentPlane plane,
            TrackHistoryRepository repository,
            ObjectMapper json,
            MeterRegistry meters) {
        return new TrackService(engine, plane, repository, json, meters);
    }

    @Bean
    @ConditionalOnProperty(
            name = "track-fusion.pipeline.enabled",
            havingValue = "true",
            matchIfMissing = true)
    NewTopic detectionsTopic(
            @Value("${track-fusion.kafka.detections-topic:detections}") String topic) {
        return TopicBuilder.name(topic).partitions(1).replicas(1).build();
    }

    @Bean
    TrackWebSocketHandler trackWebSocketHandler(
            TrackService tracks, ObjectMapper json) {
        return new TrackWebSocketHandler(tracks, json);
    }

    @Bean
    HandlerMapping webSocketMapping(TrackWebSocketHandler handler) {
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setOrder(-1);
        mapping.setUrlMap(Map.of("/ws/tracks", handler));
        return mapping;
    }

    @Bean
    WebSocketHandlerAdapter webSocketHandlerAdapter() {
        return new WebSocketHandlerAdapter();
    }
}
