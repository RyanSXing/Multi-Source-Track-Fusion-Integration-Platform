package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import java.util.Objects;
import reactor.core.publisher.Flux;

public final class ResilientSourceAdapter implements SourceAdapter {
    private final SourceAdapter delegate;
    private final CircuitBreaker circuitBreaker;

    public ResilientSourceAdapter(SourceAdapter delegate, CircuitBreaker circuitBreaker) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker");
    }

    @Override
    public String sourceId() {
        return delegate.sourceId();
    }

    @Override
    public String sourceType() {
        return delegate.sourceType();
    }

    @Override
    public Flux<Detection> stream() {
        return delegate.stream().transformDeferred(CircuitBreakerOperator.of(circuitBreaker));
    }
}
