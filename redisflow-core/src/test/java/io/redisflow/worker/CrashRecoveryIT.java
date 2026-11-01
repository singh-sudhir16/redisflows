package io.redisflow.worker;

import static io.redisflow.worker.WorkerEngineIT.handler;
import static org.assertj.core.api.Assertions.assertThat;

import io.redisflow.job.EnqueueRequest;
import io.redisflow.job.JobService;
import io.redisflow.queue.Delivery;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Chaos tests: a worker that receives jobs and dies before acknowledging them must not lose
 * any work. The "crashed" worker is simulated precisely by reading deliveries and never
 * acknowledging them, which is exactly the state Redis is left in after a kill -9.
 */
class CrashRecoveryIT extends CoreIntegrationTest {

    @Autowired
    JobService jobs;
    @Autowired
    QueueStatsService stats;

    @Test
    void noJobIsLostWhenAWorkerDiesMidBatch() {
        int total = 5_000;
        Set<String> enqueued = IntStream.range(0, total)
                .mapToObj(i -> jobs.enqueue(EnqueueRequest.of("default", "work", null)).jobId())
                .collect(Collectors.toSet());

        // The doomed worker grabs 1,000 deliveries and crashes without acknowledging any of them.
        List<Delivery.FromStream> lost = streams.read("crashed-worker", 1_000, Duration.ZERO, List.of("default"));
        assertThat(lost).hasSize(1_000);

        Map<String, Integer> executions = new ConcurrentHashMap<>();
        JobHandler work = handler("work", job -> executions.merge(job.id(), 1, Integer::sum));
        startEngine(fastSettings("survivor-1", 32, List.of("default")), work);
        startEngine(fastSettings("survivor-2", 32, List.of("default")), work);

        eventually(Duration.ofSeconds(60), () -> stats.totals().completed() == total);

        assertThat(executions.keySet()).isEqualTo(enqueued);
        assertThat(stats.queue("default").pending()).isZero();
        assertThat(jobs.deadLetters("default", 10)).isEmpty();
    }

    @Test
    void expiredPriorityLeasesAreRequeued() {
        int total = 200;
        for (int i = 0; i < total; i++) {
            jobs.enqueue(EnqueueRequest.of("default", "work", null).withPriority(1));
        }
        // A worker leases 50 critical jobs (2s visibility) and crashes.
        assertThat(priorityLane.claim(50, Duration.ofSeconds(2))).hasSize(50);

        Map<String, Integer> executions = new ConcurrentHashMap<>();
        startEngine(fastSettings("survivor", 16, List.of("default")),
                handler("work", job -> executions.merge(job.id(), 1, Integer::sum)));

        eventually(Duration.ofSeconds(30), () -> stats.totals().completed() == total);
        assertThat(executions).hasSize(total);
        assertThat(priorityLane.inflight()).isZero();
    }
}
