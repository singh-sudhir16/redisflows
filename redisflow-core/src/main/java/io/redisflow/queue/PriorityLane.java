package io.redisflow.queue;

import io.redisflow.queue.Delivery.FromPriorityLane;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Critical/high priority jobs, kept in a sorted set ordered by (priority, enqueue time).
 * Claiming leases a job instead of deleting it, giving SQS-style visibility-timeout semantics.
 */
public class PriorityLane {

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;

    public PriorityLane(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
    }

    @SuppressWarnings("unchecked")
    public List<FromPriorityLane> claim(int max, Duration lease) {
        if (max <= 0) {
            return List.of();
        }
        List<Object> members = redis.execute(scripts.priorityClaim,
                List.of(keys.priority(), keys.priorityInflight(), keys.priorityLeases()),
                Integer.toString(max), Long.toString(lease.toMillis()));
        if (members == null || members.isEmpty()) {
            return List.of();
        }
        List<FromPriorityLane> deliveries = new ArrayList<>(members.size());
        for (Object raw : members) {
            String member = raw.toString();
            deliveries.add(new FromPriorityLane(RedisKeys.queueOf(member), RedisKeys.jobIdOf(member), member));
        }
        return deliveries;
    }

    /** Re-queues jobs whose lease expired (their worker crashed or stalled). */
    public long reapExpiredLeases(int max) {
        Long reaped = redis.execute(scripts.priorityReap,
                List.of(keys.priority(), keys.priorityInflight(), keys.priorityLeases()), Integer.toString(max));
        return reaped == null ? 0 : reaped;
    }

    public long queued() {
        Long size = redis.opsForZSet().zCard(keys.priority());
        return size == null ? 0 : size;
    }

    public long inflight() {
        Long size = redis.opsForZSet().zCard(keys.priorityInflight());
        return size == null ? 0 : size;
    }
}
