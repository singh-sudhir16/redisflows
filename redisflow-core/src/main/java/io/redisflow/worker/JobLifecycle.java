package io.redisflow.worker;

import io.redisflow.config.RedisFlowProperties;
import io.redisflow.job.Job;
import io.redisflow.job.JobCodec;
import io.redisflow.queue.Delivery;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * Worker-side job state machine. Every transition (claim, complete, fail) is one Lua script
 * that updates the job hash, acknowledges the delivery, updates counters and publishes the
 * lifecycle event together, so a crash can never leave them inconsistent.
 */
public class JobLifecycle {

    public sealed interface Claim {
        record Claimed(Job job) implements Claim {
        }

        /** The job already completed or was dead-lettered; this delivery is a duplicate. */
        record AlreadyTerminal(Job job) implements Claim {
        }

        /** The job hash is gone (expired or deleted) or the message was malformed. */
        record Missing() implements Claim {
        }
    }

    public enum FailureOutcome { RETRYING, DEAD, ALREADY_TERMINAL, MISSING }

    private static final int MAX_ERROR_LENGTH = 1000;

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;
    private final String eventChannel;
    private final long completedRetentionMs;

    public JobLifecycle(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts, RedisFlowProperties props) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
        this.eventChannel = props.eventsEnabled() ? keys.events() : "";
        this.completedRetentionMs = props.jobs().completedRetention().toMillis();
    }

    @SuppressWarnings("unchecked")
    public Claim claim(Delivery delivery, String workerId) {
        if (delivery.jobId() == null || delivery.jobId().isBlank()) {
            return new Claim.Missing();
        }
        List<Object> flat = redis.execute(scripts.claimJob, List.of(keys.job(delivery.jobId())), workerId,
                eventChannel);
        if (flat == null || flat.isEmpty()) {
            return new Claim.Missing();
        }
        Job job = JobCodec.fromFlatList(flat);
        return job.status().isTerminal() ? new Claim.AlreadyTerminal(job) : new Claim.Claimed(job);
    }

    public boolean complete(Delivery delivery, String workerId) {
        List<String> ackKeys = delivery.ackKeys(keys);
        Long result = redis.execute(scripts.completeJob,
                List.of(keys.job(delivery.jobId()), ackKeys.get(0), ackKeys.get(1), keys.stats()),
                delivery.ackKind(), delivery.ackRef(), RedisKeys.CONSUMER_GROUP,
                Long.toString(completedRetentionMs), workerId, eventChannel);
        return result != null && result == 1;
    }

    public FailureOutcome fail(Delivery delivery, String error, Duration retryDelay, boolean forceDead,
                               String workerId) {
        List<String> ackKeys = delivery.ackKeys(keys);
        List<String> scriptKeys = new ArrayList<>(List.of(keys.job(delivery.jobId()), ackKeys.get(0), ackKeys.get(1),
                keys.stats(), keys.retries(), keys.deadLetters(delivery.queue())));
        String outcome = redis.execute(scripts.failJob, scriptKeys,
                delivery.ackKind(), delivery.ackRef(), RedisKeys.CONSUMER_GROUP, truncate(error),
                Long.toString(retryDelay.toMillis()), RedisKeys.member(delivery.queue(), delivery.jobId()),
                delivery.jobId(), forceDead ? "1" : "0", eventChannel, workerId);
        return switch (outcome == null ? "missing" : outcome) {
            case "retrying" -> FailureOutcome.RETRYING;
            case "dead" -> FailureOutcome.DEAD;
            case "missing" -> FailureOutcome.MISSING;
            default -> FailureOutcome.ALREADY_TERMINAL;
        };
    }

    /** Acknowledges a delivery without touching job state (duplicates, orphans). */
    public void acknowledge(Delivery delivery) {
        List<String> ackKeys = delivery.ackKeys(keys);
        redis.execute(scripts.ack, ackKeys, delivery.ackKind(), delivery.ackRef(), RedisKeys.CONSUMER_GROUP);
    }

    private static String truncate(String error) {
        if (error == null) {
            return "unknown error";
        }
        return error.length() <= MAX_ERROR_LENGTH ? error : error.substring(0, MAX_ERROR_LENGTH) + "...";
    }
}
