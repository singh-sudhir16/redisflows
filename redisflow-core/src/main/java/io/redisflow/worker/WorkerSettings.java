package io.redisflow.worker;

import io.redisflow.config.RedisFlowProperties;
import java.time.Duration;
import java.util.List;

/** Immutable runtime settings for one {@link WorkerEngine}. */
public record WorkerSettings(String id, List<String> queues, int concurrency, Duration blockTimeout,
                             Duration visibilityTimeout, Duration reclaimInterval, Duration jobTimeout,
                             Duration shutdownTimeout, Duration heartbeatInterval) {

    public WorkerSettings {
        queues = List.copyOf(queues);
        if (queues.isEmpty()) {
            throw new IllegalArgumentException("a worker needs at least one queue");
        }
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be >= 1");
        }
        // A job that is still running must never be considered lost and handed to another worker.
        if (visibilityTimeout.compareTo(jobTimeout) <= 0) {
            throw new IllegalArgumentException("visibility-timeout (" + visibilityTimeout
                    + ") must be greater than job-timeout (" + jobTimeout + ")");
        }
    }

    public static WorkerSettings from(RedisFlowProperties.Worker w) {
        return new WorkerSettings(w.resolvedId(), w.queues(), w.concurrency(), w.blockTimeout(),
                w.visibilityTimeout(), w.reclaimInterval(), w.jobTimeout(), w.shutdownTimeout(),
                w.heartbeatInterval());
    }

    public WorkerSettings withId(String newId) {
        return new WorkerSettings(newId, queues, concurrency, blockTimeout, visibilityTimeout, reclaimInterval,
                jobTimeout, shutdownTimeout, heartbeatInterval);
    }

    public WorkerSettings withConcurrency(int value) {
        return new WorkerSettings(id, queues, value, blockTimeout, visibilityTimeout, reclaimInterval, jobTimeout,
                shutdownTimeout, heartbeatInterval);
    }

    public WorkerSettings withTimeouts(Duration visibility, Duration job, Duration reclaim) {
        return new WorkerSettings(id, queues, concurrency, blockTimeout, visibility, reclaim, job, shutdownTimeout,
                heartbeatInterval);
    }
}
