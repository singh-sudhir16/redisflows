package io.redisflow.job;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import java.time.Instant;
import tools.jackson.databind.JsonNode;

/**
 * A request to run a job.
 *
 * @param queue        target queue (defaults to {@code default})
 * @param type         handler type, e.g. {@code email}
 * @param payload      arbitrary JSON passed to the handler
 * @param priority     1 (critical) .. 4 (low); 1-2 use the priority lane
 * @param maxAttempts  total attempts before the job is dead-lettered
 * @param scheduledFor run no earlier than this instant
 */
public record EnqueueRequest(
        @Pattern(regexp = QUEUE_PATTERN, message = "must match " + QUEUE_PATTERN) String queue,
        @NotBlank @Pattern(regexp = TYPE_PATTERN, message = "must match " + TYPE_PATTERN) String type,
        JsonNode payload,
        @Min(Priority.CRITICAL) @Max(Priority.LOW) Integer priority,
        @Min(1) @Max(25) Integer maxAttempts,
        Instant scheduledFor) {

    public static final String QUEUE_PATTERN = "^[A-Za-z0-9_-]{1,64}$";
    public static final String TYPE_PATTERN = "^[A-Za-z0-9_.-]{1,64}$";

    public static EnqueueRequest of(String queue, String type, JsonNode payload) {
        return new EnqueueRequest(queue, type, payload, null, null, null);
    }

    public EnqueueRequest withPriority(int value) {
        return new EnqueueRequest(queue, type, payload, value, maxAttempts, scheduledFor);
    }

    public EnqueueRequest withMaxAttempts(int value) {
        return new EnqueueRequest(queue, type, payload, priority, value, scheduledFor);
    }

    public EnqueueRequest withScheduledFor(Instant value) {
        return new EnqueueRequest(queue, type, payload, priority, maxAttempts, value);
    }
}
