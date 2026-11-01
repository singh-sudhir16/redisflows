package io.redisflow.job;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonRawValue;
import java.time.Instant;

/**
 * A unit of work. The authoritative copy lives in a Redis hash ({@code <prefix>job:<id>});
 * streams and sorted sets only carry the job id.
 *
 * @param payload raw JSON supplied by the producer, rendered as JSON (not a string) by the API
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record Job(
        String id,
        String queue,
        String type,
        @JsonRawValue String payload,
        JobStatus status,
        int priority,
        int attempts,
        int maxAttempts,
        String workerId,
        Instant createdAt,
        Instant scheduledFor,
        Instant startedAt,
        Instant completedAt,
        Instant nextRetryAt,
        String lastError) {

    /** True when the worker that claimed this delivery has used up every allowed attempt. */
    public boolean attemptsExhausted() {
        return attempts > maxAttempts;
    }
}
