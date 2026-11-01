package io.redisflow.redis;

import java.time.LocalDate;

/**
 * Every Redis key RedisFlow touches, in one place. All keys share a configurable prefix so
 * several isolated deployments (or benchmark runs) can share one Redis instance.
 */
public final class RedisKeys {

    /** Consumer group shared by all workers of a deployment. */
    public static final String CONSUMER_GROUP = "redisflow-workers";

    private final String prefix;

    public RedisKeys(String prefix) {
        this.prefix = prefix.endsWith(":") ? prefix : prefix + ":";
    }

    public String prefix() {
        return prefix;
    }

    // --- jobs and queues -------------------------------------------------

    public String job(String jobId) {
        return prefix + "job:" + jobId;
    }

    public String stream(String queue) {
        return prefix + "stream:" + queue;
    }

    public String deadLetters(String queue) {
        return prefix + "dlq:" + queue;
    }

    public String priority() {
        return prefix + "priority";
    }

    public String priorityInflight() {
        return prefix + "priority:inflight";
    }

    public String priorityLeases() {
        return prefix + "priority:leases";
    }

    public String scheduled() {
        return prefix + "scheduled";
    }

    public String retries() {
        return prefix + "retries";
    }

    public String knownQueues() {
        return prefix + "queues";
    }

    public String stats() {
        return prefix + "stats";
    }

    public String idempotency(String key) {
        return prefix + "idem:" + key;
    }

    public String events() {
        return prefix + "events";
    }

    // --- coordination ----------------------------------------------------

    public String lock(String resource) {
        return prefix + "lock:" + resource;
    }

    public String lockFence(String resource) {
        return prefix + "lockfence:" + resource;
    }

    public String rateLimit(String clientKey) {
        return prefix + "ratelimit:" + clientKey;
    }

    public String workers() {
        return prefix + "workers";
    }

    public String worker(String workerId) {
        return prefix + "worker:" + workerId;
    }

    // --- analytics -------------------------------------------------------

    public String visitors(LocalDate day) {
        return prefix + "analytics:visitors:" + day;
    }

    public String uniqueIps(LocalDate day) {
        return prefix + "analytics:ips:" + day;
    }

    public String activity(LocalDate day) {
        return prefix + "activity:" + day;
    }

    public String workerGeo() {
        return prefix + "geo:workers";
    }

    // --- members of the priority / scheduled / retry sorted sets ---------

    /** Sorted-set member encoding: {@code queue:jobId}. Queue names cannot contain ':'. */
    public static String member(String queue, String jobId) {
        return queue + ":" + jobId;
    }

    public static String queueOf(String member) {
        return member.substring(0, member.indexOf(':'));
    }

    public static String jobIdOf(String member) {
        return member.substring(member.indexOf(':') + 1);
    }
}
