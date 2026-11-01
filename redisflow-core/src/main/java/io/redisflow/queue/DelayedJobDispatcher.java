package io.redisflow.queue;

import io.redisflow.job.Priority;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Moves due jobs from the time-ordered sorted sets (scheduled jobs and retries waiting out
 * their backoff) back onto a runnable queue. Safe to run on several nodes at once: each move
 * is claimed with ZREM inside a Lua script, so a job is dispatched exactly once.
 */
public class DelayedJobDispatcher {

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;
    private final StreamQueue streams;

    public DelayedJobDispatcher(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts, StreamQueue streams) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
        this.streams = streams;
    }

    public int dispatchScheduled(int batchSize) {
        return dispatchDue(keys.scheduled(), batchSize);
    }

    public int dispatchRetries(int batchSize) {
        return dispatchDue(keys.retries(), batchSize);
    }

    public long scheduledCount() {
        return zcard(keys.scheduled());
    }

    public long retryingCount() {
        return zcard(keys.retries());
    }

    @SuppressWarnings("unchecked")
    private int dispatchDue(String zsetKey, int batchSize) {
        List<Object> due = redis.execute(scripts.dueMembers, List.of(zsetKey), Integer.toString(batchSize));
        if (due == null || due.isEmpty()) {
            return 0;
        }
        int dispatched = 0;
        String threshold = Integer.toString(Priority.PRIORITY_LANE_THRESHOLD);
        for (Object raw : due) {
            String member = raw.toString();
            String queue = RedisKeys.queueOf(member);
            String jobId = RedisKeys.jobIdOf(member);
            streams.ensureGroup(queue);
            Long result = redis.execute(scripts.dispatchDue,
                    List.of(zsetKey, keys.job(jobId), keys.stream(queue), keys.priority()),
                    member, jobId, threshold);
            if (result != null && result == 1) {
                dispatched++;
            }
        }
        return dispatched;
    }

    private long zcard(String key) {
        Long size = redis.opsForZSet().zCard(key);
        return size == null ? 0 : size;
    }
}
