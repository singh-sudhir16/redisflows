package io.redisflow.worker;

import io.redisflow.job.Job;

/**
 * Business logic for one job type. Register an implementation as a Spring bean in a worker.
 *
 * <p>Delivery is at-least-once, so handlers must be idempotent: a job may run again if a
 * worker crashes after the side effect but before the acknowledgement. Use {@link Job#id()}
 * as the idempotency key for downstream calls.
 */
public interface JobHandler {

    /** The job type this handler processes, e.g. {@code email}. */
    String type();

    /**
     * Processes the job. Returning normally completes it; throwing schedules a retry with
     * backoff, or dead-letters it immediately for {@link NonRetryableJobException}.
     */
    void handle(Job job) throws Exception;
}
