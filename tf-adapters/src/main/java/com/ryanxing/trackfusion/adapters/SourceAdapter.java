package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import reactor.core.publisher.Flux;

public interface SourceAdapter {
    String sourceId();

    String sourceType();

    Flux<Detection> stream();
}
