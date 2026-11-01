package io.redisflow.job;

import io.redisflow.config.RedisFlowProperties;
import io.redisflow.job.JobExceptions.IdempotencyConflictException;
import io.redisflow.job.JobExceptions.InvalidJobException;
import io.redisflow.job.JobExceptions.JobNotFoundException;
import io.redisflow.queue.StreamQueue;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;
import org.springframework.data.redis.core.StringRedisTemplate;

/** Producer-side API: enqueue, look up and replay jobs. */
public class JobService {

    private static final Pattern QUEUE = Pattern.compile(EnqueueRequest.QUEUE_PATTERN);
    private static final Pattern TYPE = Pattern.compile(EnqueueRequest.TYPE_PATTERN);
    private static final Pattern IDEMPOTENCY_KEY = Pattern.compile("^[\\x21-\\x7E]{1,255}$");

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;
    private final StreamQueue streams;
    private final RedisFlowProperties props;

    public JobService(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts, StreamQueue streams,
                      RedisFlowProperties props) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
        this.streams = streams;
        this.props = props;
    }

    public EnqueueResult enqueue(EnqueueRequest request) {
        return enqueue(request, null);
    }

    /**
     * Creates a job and makes it visible to workers in one atomic step.
     *
     * @param idempotencyKey optional client key; a retried request with the same key and body
     *                       returns the original job instead of creating a duplicate
     */
    @SuppressWarnings("unchecked")
    public EnqueueResult enqueue(EnqueueRequest request, String idempotencyKey) {
        String queue = request.queue() == null || request.queue().isBlank() ? "default" : request.queue();
        String type = request.type();
        int priority = request.priority() == null ? props.jobs().defaultPriority() : request.priority();
        int maxAttempts = request.maxAttempts() == null ? props.jobs().defaultMaxAttempts() : request.maxAttempts();
        String payload = request.payload() == null || request.payload().isNull() ? "{}" : request.payload().toString();
        validate(queue, type, priority, maxAttempts, idempotencyKey);

        Instant scheduledFor = request.scheduledFor();
        boolean delayed = scheduledFor != null && scheduledFor.isAfter(Instant.now());
        String mode;
        String destination;
        if (delayed) {
            mode = "scheduled";
            destination = keys.scheduled();
        } else if (Priority.usesPriorityLane(priority)) {
            mode = "priority";
            destination = keys.priority();
        } else {
            mode = "stream";
            destination = keys.stream(queue);
            streams.ensureGroup(queue);
        }

        String jobId = UUID.randomUUID().toString();
        String scheduledMillis = scheduledFor == null ? "" : Long.toString(scheduledFor.toEpochMilli());
        boolean useKey = idempotencyKey != null && !idempotencyKey.isBlank();
        long idempotencyTtl = useKey ? props.jobs().idempotencyKeyTtl().toMillis() : 0;
        String fingerprint = useKey ? fingerprint(queue, type, payload, priority, maxAttempts, scheduledMillis) : "";

        List<String> args = new ArrayList<>(List.of(
                mode, queue, jobId, Integer.toString(priority), scheduledMillis,
                Long.toString(idempotencyTtl), fingerprint, eventChannel(), type,
                "payload", payload,
                "max_attempts", Integer.toString(maxAttempts)));
        if (delayed) {
            args.add("scheduled_for");
            args.add(scheduledMillis);
        }

        List<Object> reply = redis.execute(scripts.enqueue,
                List.of(keys.job(jobId), destination, keys.knownQueues(), keys.stats(),
                        keys.idempotency(useKey ? idempotencyKey : "-")),
                args.toArray());

        String outcome = String.valueOf(reply.get(0));
        return switch (outcome) {
            case "created" -> {
                Instant createdAt = Instant.ofEpochMilli(Long.parseLong(String.valueOf(reply.get(1))));
                Job job = new Job(jobId, queue, type, payload,
                        delayed ? JobStatus.SCHEDULED : JobStatus.PENDING, priority, 0, maxAttempts, null,
                        createdAt, delayed ? scheduledFor : null, null, null, null, null);
                yield new EnqueueResult(jobId, job, false);
            }
            case "replayed" -> new EnqueueResult(String.valueOf(reply.get(1)), null, true);
            case "conflict" -> throw new IdempotencyConflictException(String.valueOf(reply.get(1)));
            default -> throw new IllegalStateException("Unexpected enqueue reply " + reply);
        };
    }

    public Optional<Job> find(String jobId) {
        Map<String, String> fields = redis.<String, String>opsForHash().entries(keys.job(jobId));
        return fields.isEmpty() ? Optional.empty() : Optional.of(JobCodec.fromMap(fields));
    }

    public Job get(String jobId) {
        return find(jobId).orElseThrow(() -> new JobNotFoundException(jobId));
    }

    /** Dead-lettered job ids, newest first. */
    public List<String> deadLetters(String queue, int limit) {
        List<String> ids = redis.opsForList().range(keys.deadLetters(queue), 0, Math.max(0, limit - 1));
        return ids == null ? List.of() : ids;
    }

    /** Moves a dead-lettered job back onto its queue with a fresh attempt budget. */
    public Job replayDeadLetter(String queue, String jobId) {
        Long result = redis.execute(scripts.replayDead,
                List.of(keys.deadLetters(queue), keys.job(jobId), keys.stream(queue), keys.priority()),
                jobId, RedisKeys.member(queue, jobId), Integer.toString(Priority.PRIORITY_LANE_THRESHOLD),
                eventChannel());
        if (result == null || result <= 0) {
            throw new JobNotFoundException(jobId + " in dead-letter queue " + queue);
        }
        streams.ensureGroup(queue);
        return get(jobId);
    }

    private String eventChannel() {
        return props.eventsEnabled() ? keys.events() : "";
    }

    private static void validate(String queue, String type, int priority, int maxAttempts, String idempotencyKey) {
        if (!QUEUE.matcher(queue).matches()) {
            throw new InvalidJobException("queue must match " + EnqueueRequest.QUEUE_PATTERN);
        }
        if (type == null || !TYPE.matcher(type).matches()) {
            throw new InvalidJobException("type must match " + EnqueueRequest.TYPE_PATTERN);
        }
        if (priority < Priority.CRITICAL || priority > Priority.LOW) {
            throw new InvalidJobException("priority must be between 1 and 4");
        }
        if (maxAttempts < 1 || maxAttempts > 25) {
            throw new InvalidJobException("max_attempts must be between 1 and 25");
        }
        if (idempotencyKey != null && !idempotencyKey.isBlank() && !IDEMPOTENCY_KEY.matcher(idempotencyKey).matches()) {
            throw new InvalidJobException("Idempotency-Key must be 1-255 printable ASCII characters");
        }
    }

    private static String sha256(String... parts) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : parts) {
                digest.update(part.getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String fingerprint(String queue, String type, String payload, int priority, int maxAttempts,
                                      String scheduledFor) {
        return sha256(queue, type, payload, Integer.toString(priority), Integer.toString(maxAttempts),
                scheduledFor);
    }
}
