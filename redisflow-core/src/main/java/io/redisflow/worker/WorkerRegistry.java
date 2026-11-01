package io.redisflow.worker;

import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Worker liveness: each worker heartbeats into a sorted set plus a short-lived info hash. */
public class WorkerRegistry {

    public record WorkerInfo(String id, String host, List<String> queues, int concurrency, int inflight,
                             long processed, long succeeded, long failed, Instant startedAt, Instant lastSeen) {
    }

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;

    public WorkerRegistry(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
    }

    public void heartbeat(WorkerInfo info, Duration ttl) {
        redis.execute(scripts.heartbeat, List.of(keys.workers(), keys.worker(info.id())),
                info.id(), Long.toString(ttl.toMillis()),
                "id", info.id(),
                "host", info.host(),
                "queues", String.join(",", info.queues()),
                "concurrency", Integer.toString(info.concurrency()),
                "inflight", Integer.toString(info.inflight()),
                "processed", Long.toString(info.processed()),
                "succeeded", Long.toString(info.succeeded()),
                "failed", Long.toString(info.failed()),
                "started_at", Long.toString(info.startedAt().toEpochMilli()));
    }

    public void remove(String workerId) {
        redis.opsForZSet().remove(keys.workers(), workerId);
        redis.delete(keys.worker(workerId));
    }

    /** Workers that sent a heartbeat within {@code window}. */
    @SuppressWarnings("unchecked")
    public List<WorkerInfo> live(Duration window) {
        List<Object> ids = redis.execute(scripts.recentMembers, List.of(keys.workers()),
                Long.toString(window.toMillis()));
        if (ids == null) {
            return List.of();
        }
        List<WorkerInfo> workers = new ArrayList<>();
        for (Object id : ids) {
            Map<Object, Object> f = redis.opsForHash().entries(keys.worker(id.toString()));
            if (!f.isEmpty()) {
                workers.add(new WorkerInfo(
                        str(f, "id"),
                        str(f, "host"),
                        Arrays.stream(str(f, "queues").split(",")).filter(s -> !s.isBlank()).toList(),
                        (int) num(f, "concurrency"),
                        (int) num(f, "inflight"),
                        num(f, "processed"),
                        num(f, "succeeded"),
                        num(f, "failed"),
                        Instant.ofEpochMilli(num(f, "started_at")),
                        Instant.ofEpochMilli(num(f, "last_seen"))));
            }
        }
        return workers;
    }

    private static String str(Map<Object, Object> f, String field) {
        Object value = f.get(field);
        return value == null ? "" : value.toString();
    }

    private static long num(Map<Object, Object> f, String field) {
        String value = str(f, field);
        return value.isEmpty() ? 0 : Long.parseLong(value);
    }
}
