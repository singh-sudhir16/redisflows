package io.redisflow.queue;

import io.redisflow.config.RedisFlowProperties;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Read-only operational view of queues for the API, dashboard and metrics. */
public class QueueStatsService {

    /** Health of one queue. Backlog is -1 when Redis cannot compute the group's lag. */
    public record QueueStats(String name, long streamLength, long backlog, long pending, long consumers,
                             long deadLetters) {
    }

    public record Totals(long enqueued, long completed, long retried, long dead) {
    }

    public record Overview(List<QueueStats> queues, long priorityQueued, long priorityInflight, long scheduled,
                           long retrying, Totals totals) {
    }

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;
    private final PriorityLane priorityLane;
    private final DelayedJobDispatcher delayed;
    private final RedisFlowProperties props;

    public QueueStatsService(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts, PriorityLane priorityLane,
                             DelayedJobDispatcher delayed, RedisFlowProperties props) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
        this.priorityLane = priorityLane;
        this.delayed = delayed;
        this.props = props;
    }

    /** Queues that have ever received a job, plus the queues workers are configured for. */
    public Set<String> knownQueues() {
        Set<String> queues = new TreeSet<>(props.worker().queues());
        Set<String> seen = redis.opsForSet().members(keys.knownQueues());
        if (seen != null) {
            queues.addAll(seen);
        }
        return queues;
    }

    @SuppressWarnings("unchecked")
    public QueueStats queue(String name) {
        List<Object> r = redis.execute(scripts.queueStats,
                List.of(keys.stream(name), keys.deadLetters(name)), RedisKeys.CONSUMER_GROUP);
        return new QueueStats(name, num(r, 0), num(r, 1), num(r, 2), num(r, 3), num(r, 4));
    }

    public Overview overview() {
        List<QueueStats> queues = knownQueues().stream().map(this::queue).toList();
        return new Overview(queues, priorityLane.queued(), priorityLane.inflight(), delayed.scheduledCount(),
                delayed.retryingCount(), totals());
    }

    public Totals totals() {
        Map<Object, Object> stats = redis.opsForHash().entries(keys.stats());
        return new Totals(stat(stats, "enqueued"), stat(stats, "completed"), stat(stats, "retried"),
                stat(stats, "dead"));
    }

    private static long stat(Map<Object, Object> stats, String field) {
        Object value = stats.get(field);
        return value == null ? 0 : Long.parseLong(value.toString());
    }

    private static long num(List<Object> values, int index) {
        if (values == null || values.size() <= index || values.get(index) == null) {
            return 0;
        }
        return Long.parseLong(values.get(index).toString());
    }
}
