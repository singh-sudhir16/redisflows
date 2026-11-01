package io.redisflow.retry;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.DoubleSupplier;

/**
 * Capped exponential backoff with "equal jitter": the delay for attempt n is
 * {@code d/2 + random(0, d/2)} where {@code d = min(cap, base * 2^(n-1))}.
 * Jitter spreads retries out so a downstream outage does not cause synchronized retry storms.
 */
public final class BackoffPolicy {

    private final Duration base;
    private final Duration cap;
    private final boolean jitter;
    private final DoubleSupplier random;

    public BackoffPolicy(Duration base, Duration cap, boolean jitter) {
        this(base, cap, jitter, () -> ThreadLocalRandom.current().nextDouble());
    }

    BackoffPolicy(Duration base, Duration cap, boolean jitter, DoubleSupplier random) {
        if (base.isNegative() || base.isZero()) {
            throw new IllegalArgumentException("base delay must be positive");
        }
        if (cap.compareTo(base) < 0) {
            throw new IllegalArgumentException("max delay must be >= base delay");
        }
        this.base = base;
        this.cap = cap;
        this.jitter = jitter;
        this.random = random;
    }

    /** Delay before the retry that follows failed attempt {@code attempt} (1-based). */
    public Duration delayAfter(int attempt) {
        int exponent = Math.max(0, Math.min(attempt - 1, 30));
        long ceiling = Math.min(cap.toMillis(), base.toMillis() * (1L << exponent));
        if (!jitter) {
            return Duration.ofMillis(ceiling);
        }
        long half = ceiling / 2;
        return Duration.ofMillis(half + (long) (random.getAsDouble() * (ceiling - half)));
    }
}
