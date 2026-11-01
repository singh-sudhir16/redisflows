package io.redisflow.redis;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

/**
 * Loads the Lua scripts from {@code classpath:redisflow/lua}. Each script is compiled together
 * with {@code prelude.lua} (shared helpers) and executed via EVALSHA, so every state transition
 * is a single atomic round trip.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class RedisScripts {

    public final RedisScript<List> enqueue = multi("enqueue");
    public final RedisScript<List> claimJob = multi("claim_job");
    public final RedisScript<Long> completeJob = integer("complete_job");
    public final RedisScript<String> failJob = text("fail_job");
    public final RedisScript<Long> ack = integer("ack");
    public final RedisScript<List> priorityClaim = multi("priority_claim");
    public final RedisScript<Long> priorityReap = integer("priority_reap");
    public final RedisScript<List> dueMembers = multi("due_members");
    public final RedisScript<Long> dispatchDue = integer("dispatch_due");
    public final RedisScript<Long> replayDead = integer("replay_dead");
    public final RedisScript<Long> lockAcquire = integer("lock_acquire");
    public final RedisScript<Long> lockRelease = integer("lock_release");
    public final RedisScript<Long> lockRenew = integer("lock_renew");
    public final RedisScript<List> rateLimit = multi("rate_limit");
    public final RedisScript<Long> streamCompact = integer("stream_compact");
    public final RedisScript<List> queueStats = multi("queue_stats");
    public final RedisScript<String> heartbeat = text("heartbeat");
    public final RedisScript<List> recentMembers = multi("recent_members");
    public final RedisScript<Long> trackVisit = integer("track_visit");

    private static final String PRELUDE = read("prelude");

    private static RedisScript<List> multi(String name) {
        return script(name, List.class);
    }

    private static RedisScript<Long> integer(String name) {
        return script(name, Long.class);
    }

    private static RedisScript<String> text(String name) {
        return script(name, String.class);
    }

    private static <T> RedisScript<T> script(String name, Class<T> resultType) {
        return new DefaultRedisScript<>(PRELUDE + "\n" + read(name), resultType);
    }

    private static String read(String name) {
        try {
            return new ClassPathResource("redisflow/lua/" + name + ".lua").getContentAsString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Missing Lua script " + name, e);
        }
    }
}
