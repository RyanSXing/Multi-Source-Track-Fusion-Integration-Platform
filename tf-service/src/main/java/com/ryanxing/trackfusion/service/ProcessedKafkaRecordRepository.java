package com.ryanxing.trackfusion.service;

import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;

public interface ProcessedKafkaRecordRepository
        extends ReactiveCrudRepository<ProcessedKafkaRecordEntity, String> {
    Flux<ProcessedKafkaRecordEntity> findByOutcomeOrderByObservedAtAsc(
            String outcome);
}
