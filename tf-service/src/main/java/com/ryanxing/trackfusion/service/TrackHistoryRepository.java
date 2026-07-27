package com.ryanxing.trackfusion.service;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.data.r2dbc.repository.Query;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TrackHistoryRepository
        extends ReactiveCrudRepository<TrackHistoryEntity, Long> {
    Flux<TrackHistoryEntity> findByTrackIdOrderByStateAtAsc(long trackId);

    @Query("SELECT COALESCE(MAX(track_id), 0) FROM track_history")
    Mono<Long> findMaxTrackId();
}
