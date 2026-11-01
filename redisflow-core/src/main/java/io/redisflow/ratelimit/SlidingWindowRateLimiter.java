package io.redisflow.ratelimit;

import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Sliding-window-log rate limiter: exact (no fixed-window boundary bursts) and atomic across
 * all API replicas because the check-and-record runs as one Lua script.
 * Memory is O(limit) per client key; a sliding-window counter would trade accuracy for O(1).
 */
public class SlidingWindowRateLimiter {

    public record Decision(boolean allowed, int limit, long remaining, Duration retryAfter) {
    }

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;
    private final int limit;
    private final Duration window;

    public SlidingWindowRateLimiter(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts, int limit,
                                    Duration window) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
        this.limit = limit;
        this.window = window;
    }

    @SuppressWarnings("unchecked")
    public Decision tryAcquire(String clientKey) {
        List<Object> r = redis.execute(scripts.rateLimit, List.of(keys.rateLimit(clientKey)),
                Long.toString(window.toMillis()), Integer.toString(limit), UUID.randomUUID().toString());
        boolean allowed = ((Number) r.get(0)).longValue() == 1;
        long remaining = ((Number) r.get(1)).longValue();
        long retryAfterMs = ((Number) r.get(2)).longValue();
        return new Decision(allowed, limit, remaining, Duration.ofMillis(retryAfterMs));
    }

    public int limit() {
        return limit;
    }

    public Duration window() {
        return window;
    }
}
