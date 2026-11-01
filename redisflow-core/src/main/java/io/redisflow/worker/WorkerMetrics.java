package io.redisflow.worker;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.redisflow.job.Job;
import java.time.Duration;
import java.util.function.Supplier;

/** Micrometer instrumentation for a worker (exported to Prometheus by the worker app). */
public final class WorkerMetrics {

    private final MeterRegistry registry;
    private final String workerId;

    public WorkerMetrics(MeterRegistry registry, String workerId, Supplier<Number> inflight) {
        this.registry = registry;
        this.workerId = workerId;
        Gauge.builder("redisflow.worker.inflight", inflight).tag("worker", workerId)
                .description("Jobs currently executing on this worker").register(registry);
    }

    /** outcome: completed | retried | dead */
    public void jobFinished(Job job, String outcome, Duration elapsed) {
        Counter.builder("redisflow.jobs").tag("queue", job.queue()).tag("type", job.type()).tag("outcome", outcome)
                .description("Jobs processed by outcome").register(registry).increment();
        Timer.builder("redisflow.job.duration").tag("type", job.type()).tag("outcome", outcome)
                .publishPercentileHistogram().description("Handler execution time")
                .register(registry).record(elapsed);
    }

    /** reason: duplicate | missing */
    public void deliverySkipped(String reason) {
        Counter.builder("redisflow.deliveries.skipped").tag("reason", reason).tag("worker", workerId)
                .register(registry).increment();
    }

    public void reclaimed(int count) {
        Counter.builder("redisflow.deliveries.reclaimed").tag("worker", workerId)
                .description("Unacknowledged deliveries taken over from silent consumers")
                .register(registry).increment(count);
    }

    public void fetchError() {
        Counter.builder("redisflow.fetch.errors").tag("worker", workerId).register(registry).increment();
    }
}
