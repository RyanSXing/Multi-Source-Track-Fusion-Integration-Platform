package com.ryanxing.trackfusion.service;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("track_history")
public record TrackHistoryEntity(
        @Id Long id,
        String sessionId,
        long trackId,
        String status,
        Instant stateAt,
        Instant lastObservedAt,
        double latDeg,
        double lonDeg,
        Double altMeters,
        double eastVelocityMps,
        double northVelocityMps,
        int hitCount,
        int consecutiveMisses,
        String contributorsJson) {}
