package io.redisflow.analytics;

import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Unique visitor / IP counts with HyperLogLog: ~0.81% standard error in at most 12 KB per day,
 * however many visitors there are. PFCOUNT over several daily keys returns the size of their
 * union, so weekly uniques need no extra storage.
 */
public class VisitorAnalytics {

    private static final long RETENTION_SECONDS = Duration.ofDays(35).toSeconds();

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;

    public VisitorAnalytics(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
    }

    /**
     * Records one request in a single round trip. Runs on the shared multiplexed connection
     * (a Lua script, not a pipeline), so the request path never waits on the connection pool.
     */
    public void track(String visitorId, String ip) {
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        redis.execute(scripts.trackVisit, List.of(keys.visitors(today), keys.uniqueIps(today)),
                visitorId == null ? "" : visitorId, ip == null ? "" : ip, Long.toString(RETENTION_SECONDS));
    }

    public long uniqueVisitors(LocalDate from, LocalDate to) {
        return count(dailyKeys(from, to, true));
    }

    public long uniqueIps(LocalDate from, LocalDate to) {
        return count(dailyKeys(from, to, false));
    }

    private long count(String[] dailyKeys) {
        Long count = redis.opsForHyperLogLog().size(dailyKeys);
        return count == null ? 0 : count;
    }

    private String[] dailyKeys(LocalDate from, LocalDate to, boolean visitors) {
        if (to.isBefore(from)) {
            throw new IllegalArgumentException("'to' must not be before 'from'");
        }
        if (from.plusDays(31).isBefore(to)) {
            throw new IllegalArgumentException("range is limited to 31 days");
        }
        List<String> result = new ArrayList<>();
        for (LocalDate day = from; !day.isAfter(to); day = day.plusDays(1)) {
            result.add(visitors ? keys.visitors(day) : keys.uniqueIps(day));
        }
        return result.toArray(String[]::new);
    }
}
