package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import reactor.core.publisher.Mono;

public interface TrackEnricher {
    String sourceType();

    Mono<Detection> enrich(Detection detection);
}
