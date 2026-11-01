package io.redisflow.worker;

import static org.assertj.core.api.Assertions.assertThat;

import io.redisflow.job.EnqueueRequest;
import io.redisflow.job.Job;
import io.redisflow.job.JobService;
import io.redisflow.job.JobStatus;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class WorkerEngineIT extends CoreIntegrationTest {

    @Autowired
    JobService jobs;
    @Autowired
    QueueStatsService stats;

    @Test
    void processesEveryJobExactlyOnceUnderConcurrency() {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        startEngine(fastSettings("w1", 16, List.of("default")),
                handler("noop", job -> runs.computeIfAbsent(job.id(), k -> new AtomicInteger()).incrementAndGet()));

        for (int i = 0; i < 500; i++) {
            jobs.enqueue(EnqueueRequest.of("default", "noop", null));
        }

        eventually(Duration.ofSeconds(20), () -> stats.totals().completed() == 500);
        assertThat(runs).hasSize(500);
        assertThat(runs.values()).allMatch(count -> count.get() == 1);
        assertThat(stats.queue("default").pending()).isZero();
    }

    @Test
    void retriesWithBackoffUntilTheHandlerSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        startEngine(fastSettings("w1", 2, List.of("default")), handler("flaky", job -> {
            if (calls.incrementAndGet() < 3) {
                throw new IllegalStateException("downstream unavailable");
            }
        }));

        String id = jobs.enqueue(EnqueueRequest.of("default", "flaky", null).withMaxAttempts(5)).jobId();

        eventually(Duration.ofSeconds(10), () -> jobs.get(id).status() == JobStatus.COMPLETED);
        Job job = jobs.get(id);
        assertThat(job.attempts()).isEqualTo(3);
        assertThat(job.lastError()).isNull();
        assertThat(stats.totals().retried()).isEqualTo(2);
    }

    @Test
    void deadLettersAJobOnceAttemptsAreExhausted() {
        startEngine(fastSettings("w1", 2, List.of("default")), handler("broken", job -> {
            throw new IllegalStateException("always fails");
        }));

        String id = jobs.enqueue(EnqueueRequest.of("default", "broken", null).withMaxAttempts(3)).jobId();

        eventually(Duration.ofSeconds(10), () -> jobs.get(id).status() == JobStatus.DEAD);
        Job job = jobs.get(id);
        assertThat(job.attempts()).isEqualTo(3);
        assertThat(job.lastError()).contains("always fails");
        assertThat(jobs.deadLetters("default", 10)).containsExactly(id);
    }

    @Test
    void nonRetryableFailureSkipsRetries() {
        startEngine(fastSettings("w1", 2, List.of("default")), handler("validate", job -> {
            throw new NonRetryableJobException("invalid payload");
        }));

        String id = jobs.enqueue(EnqueueRequest.of("default", "validate", null).withMaxAttempts(5)).jobId();

        eventually(Duration.ofSeconds(5), () -> jobs.get(id).status() == JobStatus.DEAD);
        assertThat(jobs.get(id).attempts()).isEqualTo(1);
    }

    @Test
    void deadLetteredJobCanBeReplayed() {
        AtomicInteger calls = new AtomicInteger();
        startEngine(fastSettings("w1", 2, List.of("default")), handler("once-broken", job -> {
            if (calls.incrementAndGet() == 1) {
                throw new NonRetryableJobException("bad config");
            }
        }));
        String id = jobs.enqueue(EnqueueRequest.of("default", "once-broken", null)).jobId();
        eventually(Duration.ofSeconds(5), () -> jobs.get(id).status() == JobStatus.DEAD);

        jobs.replayDeadLetter("default", id);

        eventually(Duration.ofSeconds(5), () -> jobs.get(id).status() == JobStatus.COMPLETED);
        assertThat(jobs.deadLetters("default", 10)).isEmpty();
    }

    @Test
    void criticalJobsJumpAheadOfTheBacklog() throws Exception {
        for (int i = 0; i < 20; i++) {
            jobs.enqueue(EnqueueRequest.of("default", "record", null));
        }
        for (int i = 0; i < 5; i++) {
            jobs.enqueue(EnqueueRequest.of("default", "record", null).withPriority(1));
        }
        List<Integer> order = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(25);
        startEngine(fastSettings("w1", 1, List.of("default")), handler("record", job -> {
            order.add(job.priority());
            done.countDown();
        }));

        assertThat(done.await(10, TimeUnit.SECONDS)).isTrue();
        assertThat(order.subList(0, 5)).containsOnly(1);
    }

    @Test
    void timedOutJobIsRetried() {
        AtomicInteger calls = new AtomicInteger();
        startEngine(fastSettings("w1", 2, List.of("default")), handler("slow-once", job -> {
            if (calls.incrementAndGet() == 1) {
                Thread.sleep(3_000); // longer than the 1s job timeout
            }
        }));

        String id = jobs.enqueue(EnqueueRequest.of("default", "slow-once", null)).jobId();

        eventually(Duration.ofSeconds(10), () -> jobs.get(id).status() == JobStatus.COMPLETED);
        assertThat(jobs.get(id).attempts()).isEqualTo(2);
    }

    @Test
    void scheduledJobRunsOnlyOnceDue() {
        startEngine(fastSettings("w1", 2, List.of("default")), handler("report", job -> { }));

        String id = jobs.enqueue(EnqueueRequest.of("default", "report", null)
                .withScheduledFor(java.time.Instant.now().plusMillis(1_500))).jobId();

        assertThat(jobs.get(id).status()).isEqualTo(JobStatus.SCHEDULED);
        eventually(Duration.ofSeconds(5), () -> jobs.get(id).status() == JobStatus.COMPLETED);
        Job job = jobs.get(id);
        assertThat(job.startedAt()).isAfterOrEqualTo(job.scheduledFor());
    }

    @Test
    void gracefulShutdownLetsInFlightJobsFinish() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        WorkerEngine engine = startEngine(fastSettings("w1", 1, List.of("default")), handler("slow", job -> {
            started.countDown();
            Thread.sleep(500);
        }));
        String id = jobs.enqueue(EnqueueRequest.of("default", "slow", null)).jobId();
        assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

        engine.stop();

        assertThat(jobs.get(id).status()).isEqualTo(JobStatus.COMPLETED);
    }

    interface Body {
        void run(Job job) throws Exception;
    }

    static JobHandler handler(String type, Body body) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) throws Exception {
                body.run(job);
            }
        };
    }
}
