package io.redisflow.maintenance;

import io.redisflow.config.RedisFlowProperties;
import io.redisflow.lock.LeaderElection;
import io.redisflow.queue.DelayedJobDispatcher;
import io.redisflow.queue.PriorityLane;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.queue.StreamQueue;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

/**
 * Background duties that should run on one node at a time: dispatching due scheduled jobs and
 * retries, re-queueing expired priority leases, and compacting streams.
 *
 * <p>One node is elected leader through a Redis lease. Leadership is an efficiency measure, not a
 * correctness one: every duty is an idempotent, atomically-claimed Lua operation, so a brief
 * overlap during failover cannot dispatch a job twice or lose one.
 */
public class MaintenanceService implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(MaintenanceService.class);
    private static final int MAX_BATCHES_PER_TICK = 10;

    private final LeaderElection election;
    private final DelayedJobDispatcher delayed;
    private final PriorityLane priorityLane;
    private final StreamQueue streams;
    private final QueueStatsService queues;
    private final RedisFlowProperties.Maintenance settings;
    private final long electionIntervalNanos;

    private volatile boolean running;
    private ScheduledExecutorService executor;
    private long lastElectionNanos;
    private long lastCompactionNanos;

    public MaintenanceService(LeaderElection election, DelayedJobDispatcher delayed, PriorityLane priorityLane,
                              StreamQueue streams, QueueStatsService queues,
                              RedisFlowProperties.Maintenance settings) {
        this.election = election;
        this.delayed = delayed;
        this.priorityLane = priorityLane;
        this.streams = streams;
        this.queues = queues;
        this.settings = settings;
        // Renew (or try to win) the lease three times per TTL.
        this.electionIntervalNanos = settings.leaderLeaseTtl().dividedBy(3).toNanos();
    }

    @Override
    public synchronized void start() {
        if (running) {
            return;
        }
        executor = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().daemon().name("rf-maintenance").factory());
        running = true;
        lastElectionNanos = System.nanoTime() - electionIntervalNanos;
        lastCompactionNanos = System.nanoTime();
        executor.scheduleWithFixedDelay(this::tickSafely, 0, settings.pollInterval().toMillis(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        executor.shutdown();
        try {
            executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        election.resign();
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    public boolean isLeader() {
        return election.isLeader();
    }

    private void tickSafely() {
        try {
            tick();
        } catch (RuntimeException e) {
            log.warn("Maintenance tick failed: {}", e.getMessage());
        }
    }

    /** Package-visible for tests: one maintenance pass. */
    void tick() {
        long now = System.nanoTime();
        if (now - lastElectionNanos >= electionIntervalNanos) {
            lastElectionNanos = now;
            election.tick();
        }
        if (!election.isLeader()) {
            return;
        }
        int batch = settings.batchSize();
        drain(() -> delayed.dispatchScheduled(batch), batch);
        drain(() -> delayed.dispatchRetries(batch), batch);
        drain(() -> (int) priorityLane.reapExpiredLeases(batch), batch);

        if (now - lastCompactionNanos >= settings.compactionInterval().toNanos()) {
            lastCompactionNanos = now;
            for (String queue : queues.knownQueues()) {
                long removed = streams.compact(queue);
                if (removed > 0) {
                    log.debug("Compacted {} acknowledged entries from queue '{}'", removed, queue);
                }
            }
        }
    }

    private void drain(java.util.function.IntSupplier step, int batch) {
        for (int i = 0; i < MAX_BATCHES_PER_TICK && running; i++) {
            if (step.getAsInt() < batch) {
                return;
            }
        }
    }

    public Duration pollInterval() {
        return settings.pollInterval();
    }
}
