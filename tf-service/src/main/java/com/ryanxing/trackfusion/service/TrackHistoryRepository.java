package com.ryanxing.trackfusion.service;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface TrackHistoryRepository
        extends ReactiveCrudRepository<TrackHistoryEntity, Long> {
    Flux<TrackHistoryEntity> findByTrackIdOrderByStateAtAsc(long trackId);
}
