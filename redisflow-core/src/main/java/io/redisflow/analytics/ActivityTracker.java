package io.redisflow.analytics;

import io.redisflow.redis.RedisKeys;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Daily active users with one bitmap per day (bit N = user N was active): 1 M users take
 * ~122 KB per day. BITOP AND across days answers retention questions such as
 * "how many users were active on every day this week".
 */
public class ActivityTracker {

    /** Bitmaps suit dense numeric ids; cap the offset so a bad id cannot allocate hundreds of MB. */
    public static final long MAX_USER_ID = 100_000_000L;
    private static final Duration RETENTION = Duration.ofDays(35);

    private final StringRedisTemplate redis;
    private final RedisKeys keys;

    public ActivityTracker(StringRedisTemplate redis, RedisKeys keys) {
        this.redis = redis;
        this.keys = keys;
    }

    public void markActive(long userId) {
        requireValid(userId);
        String key = keys.activity(today());
        redis.opsForValue().setBit(key, userId, true);
        redis.expire(key, RETENTION);
    }

    public boolean isActive(long userId, LocalDate day) {
        requireValid(userId);
        return Boolean.TRUE.equals(redis.opsForValue().getBit(keys.activity(day), userId));
    }

    public long activeCount(LocalDate day) {
        byte[] key = bytes(keys.activity(day));
        Long count = redis.execute((RedisCallback<Long>) c -> c.stringCommands().bitCount(key));
        return count == null ? 0 : count;
    }

    /** Users active on every one of the given days. */
    public long activeOnAllDays(List<LocalDate> days) {
        if (days.isEmpty()) {
            return 0;
        }
        byte[] destination = bytes(keys.prefix() + "tmp:activity:" + UUID.randomUUID());
        byte[][] sources = days.stream().map(d -> bytes(keys.activity(d))).toArray(byte[][]::new);
        Long count = redis.execute((RedisCallback<Long>) c -> {
            c.stringCommands().bitOp(BitOperation.AND, destination, sources);
            Long result = c.stringCommands().bitCount(destination);
            c.keyCommands().del(destination);
            return result;
        });
        return count == null ? 0 : count;
    }

    public static LocalDate today() {
        return LocalDate.now(ZoneOffset.UTC);
    }

    private static void requireValid(long userId) {
        if (userId < 0 || userId > MAX_USER_ID) {
            throw new IllegalArgumentException("user id must be between 0 and " + MAX_USER_ID);
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
