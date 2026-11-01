package io.redisflow.job;

/** Domain exceptions surfaced by the job API. */
public final class JobExceptions {

    private JobExceptions() {
    }

    public static final class JobNotFoundException extends RuntimeException {
        public JobNotFoundException(String jobId) {
            super("Job not found: " + jobId);
        }
    }

    public static final class InvalidJobException extends RuntimeException {
        public InvalidJobException(String message) {
            super(message);
        }
    }

    /** The idempotency key was already used for a request with a different body. */
    public static final class IdempotencyConflictException extends RuntimeException {
        private final String existingJobId;

        public IdempotencyConflictException(String existingJobId) {
            super("Idempotency key was already used with a different request (job " + existingJobId + ")");
            this.existingJobId = existingJobId;
        }

        public String existingJobId() {
            return existingJobId;
        }
    }
}
