package io.redisflow.worker;

import io.micrometer.core.instrument.MeterRegistry;
import io.redisflow.events.EventPublisher;
import io.redisflow.job.Job;
import io.redisflow.queue.Delivery;
import io.redisflow.queue.Delivery.FromStream;
import io.redisflow.queue.PriorityLane;
import io.redisflow.queue.StreamQueue;
import io.redisflow.retry.BackoffPolicy;
import io.redisflow.worker.JobLifecycle.Claim;
import io.redisflow.worker.JobLifecycle.FailureOutcome;
import io.redisflow.worker.WorkerRegistry.WorkerInfo;
import java.net.InetAddress;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Consumes jobs and runs them on virtual threads.
 *
 * <ul>
 *   <li><b>Fetch with backpressure</b>: a single fetcher thread asks for only as many jobs as there
 *       are free slots ({@link Semaphore} of size {@code concurrency}), checking the priority lane
 *       first and then blocking on XREADGROUP across all queues.</li>
 *   <li><b>Execution</b>: each job runs on its own virtual thread with a hard timeout.</li>
 *   <li><b>Crash recovery</b>: a housekeeping task claims deliveries other consumers left
 *       unacknowledged for longer than the visibility timeout.</li>
 *   <li><b>Graceful shutdown</b>: stops fetching, lets in-flight jobs finish (bounded by
 *       {@code shutdownTimeout}); anything still running is redelivered elsewhere later.</li>
 * </ul>
 */
public class WorkerEngine implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(WorkerEngine.class);
    private static final Duration MAX_ERROR_BACKOFF = Duration.ofSeconds(5);

    private final WorkerSettings settings;
    private final StreamQueue streams;
    private final PriorityLane priorityLane;
    private final JobLifecycle lifecycle;
    private final JobHandlerRegistry handlers;
    private final BackoffPolicy backoff;
    private final WorkerRegistry registry;
    private final EventPublisher events;
    private final WorkerMetrics metrics;

    private final Semaphore slots;
    private final AtomicInteger inflight = new AtomicInteger();
    private final AtomicLong processed = new AtomicLong();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final String host = hostName();

    private volatile boolean running;
    private volatile Instant startedAt;
    private ExecutorService jobExecutor;
    private ExecutorService handlerExecutor;
    private ScheduledExecutorService housekeeping;
    private Thread fetcher;

    public WorkerEngine(WorkerSettings settings, StreamQueue streams, PriorityLane priorityLane,
                        JobLifecycle lifecycle, JobHandlerRegistry handlers, BackoffPolicy backoff,
                        WorkerRegistry registry, EventPublisher events, MeterRegistry meterRegistry) {
        this.settings = settings;
        this.streams = streams;
        this.priorityLane = priorityLane;
        this.lifecycle = lifecycle;
        this.handlers = handlers;
        this.backoff = backoff;
        this.registry = registry;
        this.events = events;
        this.metrics = new WorkerMetrics(meterRegistry, settings.id(), inflight::get);
        this.slots = new Semaphore(settings.concurrency());
    }

    // --- lifecycle ---------------------------------------------------------

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        settings.queues().forEach(streams::ensureGroup);
        String id = settings.id();
        jobExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("rf-job-", 0).factory());
        handlerExecutor = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("rf-handler-", 0).factory());
        housekeeping = Executors.newScheduledThreadPool(2,
                Thread.ofPlatform().daemon().name("rf-housekeeping-" + id + "-", 0).factory());
        startedAt = Instant.now();
        running = true;

        fetcher = Thread.ofPlatform().name("rf-fetcher-" + id).start(this::fetchLoop);
        long reclaimMs = settings.reclaimInterval().toMillis();
        housekeeping.scheduleWithFixedDelay(this::reclaimSafely, reclaimMs, reclaimMs, TimeUnit.MILLISECONDS);
        housekeeping.scheduleAtFixedRate(this::heartbeatSafely, 0, settings.heartbeatInterval().toMillis(),
                TimeUnit.MILLISECONDS);

        events.workerEvent("worker.started", id);
        log.info("Worker {} started: queues={} concurrency={} handlers={}", id, settings.queues(),
                settings.concurrency(), handlers.types());
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        log.info("Worker {} stopping: waiting up to {} for {} in-flight jobs", settings.id(),
                settings.shutdownTimeout(), inflight.get());
        try {
            fetcher.join(settings.blockTimeout().plusSeconds(5).toMillis());
            housekeeping.shutdownNow();
            jobExecutor.shutdown();
            if (!jobExecutor.awaitTermination(settings.shutdownTimeout().toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Worker {}: {} jobs did not finish in time; they will be redelivered after the "
                        + "visibility timeout", settings.id(), inflight.get());
                jobExecutor.shutdownNow();
            }
            handlerExecutor.shutdownNow();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        try {
            registry.remove(settings.id());
        } catch (RuntimeException e) {
            log.debug("Could not deregister worker: {}", e.getMessage());
        }
        events.workerEvent("worker.stopped", settings.id());
        log.info("Worker {} stopped (processed={} succeeded={} failed={})", settings.id(), processed.get(),
                succeeded.get(), failed.get());
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    // --- fetching ----------------------------------------------------------

    private void fetchLoop() {
        Duration errorBackoff = Duration.ofMillis(100);
        while (running) {
            try {
                int free = awaitFreeSlots();
                if (!running) {
                    break;
                }
                List<Delivery> batch = new ArrayList<>(priorityLane.claim(free, settings.visibilityTimeout()));
                int remaining = free - batch.size();
                if (remaining > 0) {
                    // Block only when there is nothing to do; otherwise just top up the batch.
                    Duration block = batch.isEmpty() ? settings.blockTimeout() : Duration.ZERO;
                    batch.addAll(streams.read(settings.id(), remaining, block, settings.queues()));
                }
                for (Delivery delivery : batch) {
                    dispatch(delivery);
                }
                errorBackoff = Duration.ofMillis(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                if (!running) {
                    return;
                }
                metrics.fetchError();
                log.warn("Worker {}: fetch failed ({}), retrying in {}", settings.id(), e.getMessage(), errorBackoff);
                sleepQuietly(errorBackoff);
                errorBackoff = errorBackoff.multipliedBy(2).compareTo(MAX_ERROR_BACKOFF) > 0
                        ? MAX_ERROR_BACKOFF : errorBackoff.multipliedBy(2);
            }
        }
    }

    /** Waits for at least one free slot and returns how many are free. */
    private int awaitFreeSlots() throws InterruptedException {
        slots.acquire();
        slots.release();
        return Math.max(1, slots.availablePermits());
    }

    /** Hands a delivery to a virtual thread, waiting for a slot first (backpressure). */
    private void dispatch(Delivery delivery) throws InterruptedException {
        slots.acquire();
        inflight.incrementAndGet();
        try {
            jobExecutor.execute(() -> {
                try {
                    process(delivery);
                } finally {
                    inflight.decrementAndGet();
                    slots.release();
                }
            });
        } catch (RejectedExecutionException e) {
            // Shutting down: leave the delivery unacknowledged so another worker picks it up.
            inflight.decrementAndGet();
            slots.release();
        }
    }

    // --- processing --------------------------------------------------------

    private void process(Delivery delivery) {
        Claim claim;
        try {
            claim = lifecycle.claim(delivery, settings.id());
        } catch (RuntimeException e) {
            log.warn("Could not claim job {}: {} (it will be redelivered)", delivery.jobId(), e.getMessage());
            return;
        }
        switch (claim) {
            case Claim.Missing missing -> {
                lifecycle.acknowledge(delivery);
                metrics.deliverySkipped("missing");
            }
            case Claim.AlreadyTerminal terminal -> {
                lifecycle.acknowledge(delivery);
                metrics.deliverySkipped("duplicate");
            }
            case Claim.Claimed claimed -> run(delivery, claimed.job());
        }
    }

    private void run(Delivery delivery, Job job) {
        processed.incrementAndGet();
        long start = System.nanoTime();

        if (job.attemptsExhausted()) {
            // Earlier deliveries never reported back (crashed worker), so do not run it again.
            fail(delivery, job, "attempts exhausted: previous deliveries did not finish", true, start);
            return;
        }
        Optional<JobHandler> handler = handlers.find(job.type());
        if (handler.isEmpty()) {
            fail(delivery, job, "no handler registered for job type '" + job.type() + "'", true, start);
            return;
        }

        try {
            execute(handler.get(), job);
        } catch (NonRetryableJobException e) {
            fail(delivery, job, describe(e), true, start);
            return;
        } catch (TimeoutException e) {
            fail(delivery, job, "timed out after " + settings.jobTimeout(), false, start);
            return;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return; // shutting down; the delivery stays pending and is redelivered
        } catch (Exception e) {
            fail(delivery, job, describe(e), false, start);
            return;
        }

        try {
            lifecycle.complete(delivery, settings.id());
            succeeded.incrementAndGet();
            metrics.jobFinished(job, "completed", elapsedSince(start));
        } catch (RuntimeException e) {
            log.error("Job {} succeeded but could not be marked complete: {} (it will be redelivered)", job.id(),
                    e.getMessage());
        }
    }

    private void execute(JobHandler handler, Job job) throws Exception {
        Future<?> future = handlerExecutor.submit(() -> {
            handler.handle(job);
            return null;
        });
        try {
            future.get(settings.jobTimeout().toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            throw new RuntimeException(cause);
        } catch (TimeoutException | InterruptedException e) {
            future.cancel(true);
            throw e;
        }
    }

    private void fail(Delivery delivery, Job job, String error, boolean forceDead, long start) {
        failed.incrementAndGet();
        try {
            FailureOutcome outcome = lifecycle.fail(delivery, error, backoff.delayAfter(job.attempts()), forceDead,
                    settings.id());
            switch (outcome) {
                case RETRYING -> {
                    metrics.jobFinished(job, "retried", elapsedSince(start));
                    log.info("Job {} ({}) attempt {}/{} failed, will retry: {}", job.id(), job.type(), job.attempts(),
                            job.maxAttempts(), error);
                }
                case DEAD -> {
                    metrics.jobFinished(job, "dead", elapsedSince(start));
                    log.warn("Job {} ({}) dead-lettered after {} attempts: {}", job.id(), job.type(), job.attempts(),
                            error);
                }
                default -> log.debug("Job {} was already {} when failing", job.id(), outcome);
            }
        } catch (RuntimeException e) {
            log.error("Could not record failure for job {}: {} (it will be redelivered)", job.id(), e.getMessage());
        }
    }

    // --- housekeeping ------------------------------------------------------

    private void reclaimSafely() {
        try {
            int budget = slots.availablePermits();
            for (String queue : settings.queues()) {
                if (budget <= 0 || !running) {
                    return;
                }
                List<FromStream> claimed = streams.reclaim(queue, settings.id(), settings.visibilityTimeout(), budget);
                if (!claimed.isEmpty()) {
                    log.warn("Worker {} reclaimed {} stale deliveries from queue '{}'", settings.id(), claimed.size(),
                            queue);
                    metrics.reclaimed(claimed.size());
                }
                for (FromStream delivery : claimed) {
                    dispatch(delivery);
                }
                budget -= claimed.size();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            log.warn("Reclaim failed: {}", e.getMessage());
        }
    }

    private void heartbeatSafely() {
        try {
            registry.heartbeat(new WorkerInfo(settings.id(), host, settings.queues(), settings.concurrency(),
                    inflight.get(), processed.get(), succeeded.get(), failed.get(), startedAt, Instant.now()),
                    settings.heartbeatInterval().multipliedBy(3));
        } catch (RuntimeException e) {
            log.debug("Heartbeat failed: {}", e.getMessage());
        }
    }

    // --- accessors ---------------------------------------------------------

    public String workerId() {
        return settings.id();
    }

    public int inflight() {
        return inflight.get();
    }

    public long processedCount() {
        return processed.get();
    }

    public long succeededCount() {
        return succeeded.get();
    }

    public long failedCount() {
        return failed.get();
    }

    private static Duration elapsedSince(long startNanos) {
        return Duration.ofNanos(System.nanoTime() - startNanos);
    }

    private static String describe(Exception e) {
        String message = e.getMessage();
        return e.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private static void sleepQuietly(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String hostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
