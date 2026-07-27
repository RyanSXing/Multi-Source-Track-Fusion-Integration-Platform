package com.ryanxing.trackfusion.adapters;

import com.ryanxing.trackfusion.common.Detection;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

public final class ResilientSourceAdapter implements SourceAdapter {
    private final SourceAdapter delegate;
    private final CircuitBreaker circuitBreaker;
    private final Duration minimumBackoff;
    private final Duration maximumBackoff;
    private final Duration repeatDelay;
    private final Consumer<Throwable> retryObserver;

    public ResilientSourceAdapter(SourceAdapter delegate, CircuitBreaker circuitBreaker) {
        this(delegate, circuitBreaker, null, null, null, ignored -> {});
    }

    public ResilientSourceAdapter(
            SourceAdapter delegate,
            CircuitBreaker circuitBreaker,
            Duration minimumBackoff,
            Duration maximumBackoff) {
        this(delegate, circuitBreaker, minimumBackoff, maximumBackoff, null);
    }

    public ResilientSourceAdapter(
            SourceAdapter delegate,
            CircuitBreaker circuitBreaker,
            Duration minimumBackoff,
            Duration maximumBackoff,
            Duration repeatDelay) {
        this(
                delegate,
                circuitBreaker,
                minimumBackoff,
                maximumBackoff,
                repeatDelay,
                ignored -> {});
    }

    public ResilientSourceAdapter(
            SourceAdapter delegate,
            CircuitBreaker circuitBreaker,
            Duration minimumBackoff,
            Duration maximumBackoff,
            Duration repeatDelay,
            Consumer<Throwable> retryObserver) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        this.circuitBreaker = Objects.requireNonNull(circuitBreaker, "circuitBreaker");
        if (minimumBackoff != null
                && (minimumBackoff.isNegative()
                        || minimumBackoff.isZero()
                        || maximumBackoff == null
                        || maximumBackoff.compareTo(minimumBackoff) < 0)) {
            throw new IllegalArgumentException("invalid retry backoff");
        }
        if (minimumBackoff == null && maximumBackoff != null) {
            throw new IllegalArgumentException("minimumBackoff is required");
        }
        if (repeatDelay != null && (repeatDelay.isNegative() || repeatDelay.isZero())) {
            throw new IllegalArgumentException("repeatDelay must be positive");
        }
        this.minimumBackoff = minimumBackoff;
        this.maximumBackoff = maximumBackoff;
        this.repeatDelay = repeatDelay;
        this.retryObserver = Objects.requireNonNull(retryObserver, "retryObserver");
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
        Flux<Detection> stream =
                Flux.defer(
                        () ->
                                delegate.stream()
                                        .transformDeferred(
                                                CircuitBreakerOperator.of(circuitBreaker)));
        if (minimumBackoff != null) {
            stream =
                    stream.retryWhen(
                            Retry.backoff(Long.MAX_VALUE, minimumBackoff)
                                    .maxBackoff(maximumBackoff)
                                    .transientErrors(true)
                                    .doBeforeRetry(
                                            retry -> retryObserver.accept(retry.failure())));
        }
        return repeatDelay == null
                ? stream
                : stream.repeatWhen(completions -> completions.delayElements(repeatDelay));
    }
}
