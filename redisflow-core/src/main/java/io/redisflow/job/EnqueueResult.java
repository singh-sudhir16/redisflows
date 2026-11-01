package io.redisflow.job;

/**
 * Outcome of an enqueue call.
 *
 * @param jobId    id of the created job, or of the original job when replayed
 * @param job      the job as created (null when replayed; fetch it by id if needed)
 * @param replayed true when an idempotency key matched an earlier identical request
 */
public record EnqueueResult(String jobId, Job job, boolean replayed) {
}
