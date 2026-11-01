package io.redisflow.lock;

import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Single-instance Redis lock: {@code SET NX PX} to acquire, compare-and-delete / compare-and-expire
 * Lua scripts to release and renew (so a holder can never release someone else's lock), and a
 * fencing token from a per-resource counter.
 *
 * <p>This is a lease, not a consensus protocol: it assumes one Redis primary. Use the fencing
 * token when the protected resource can verify it.
 */
public class DistributedLockService {

    /** A held lock. {@code fencingToken} increases on every successful acquisition of the resource. */
    public record LockHandle(String resource, String holder, long fencingToken, Duration ttl) {
    }

    public record LockInfo(String resource, String holder, Duration remainingTtl, long lastFencingToken) {
    }

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;

    public DistributedLockService(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
    }

    public Optional<LockHandle> tryAcquire(String resource, String holder, Duration ttl) {
        requireValid(resource, holder, ttl);
        Long token = redis.execute(scripts.lockAcquire, List.of(keys.lock(resource), keys.lockFence(resource)),
                holder, Long.toString(ttl.toMillis()));
        if (token == null || token == 0) {
            return Optional.empty();
        }
        return Optional.of(new LockHandle(resource, holder, token, ttl));
    }

    public boolean release(String resource, String holder) {
        Long released = redis.execute(scripts.lockRelease, List.of(keys.lock(resource)), holder);
        return released != null && released == 1;
    }

    public boolean release(LockHandle handle) {
        return release(handle.resource(), handle.holder());
    }

    public boolean renew(String resource, String holder, Duration ttl) {
        Long renewed = redis.execute(scripts.lockRenew, List.of(keys.lock(resource)), holder,
                Long.toString(ttl.toMillis()));
        return renewed != null && renewed == 1;
    }

    public Optional<LockInfo> inspect(String resource) {
        String holder = redis.opsForValue().get(keys.lock(resource));
        if (holder == null) {
            return Optional.empty();
        }
        Long ttlMillis = redis.getExpire(keys.lock(resource), TimeUnit.MILLISECONDS);
        String fence = redis.opsForValue().get(keys.lockFence(resource));
        return Optional.of(new LockInfo(resource, holder,
                Duration.ofMillis(ttlMillis == null || ttlMillis < 0 ? 0 : ttlMillis),
                fence == null ? 0 : Long.parseLong(fence)));
    }

    private static void requireValid(String resource, String holder, Duration ttl) {
        if (resource == null || resource.isBlank() || holder == null || holder.isBlank()) {
            throw new IllegalArgumentException("resource and holder are required");
        }
        if (ttl.isNegative() || ttl.isZero()) {
            throw new IllegalArgumentException("ttl must be positive");
        }
    }
}
