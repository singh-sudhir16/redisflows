package io.redisflow.queue;

import static org.assertj.core.api.Assertions.assertThat;

import io.redisflow.job.EnqueueRequest;
import io.redisflow.job.JobService;
import io.redisflow.redis.RedisKeys;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class StreamCompactionIT extends CoreIntegrationTest {

    @Autowired
    JobService jobs;
    @Autowired
    RedisKeys keys;

    @Test
    void compactionRemovesOnlyAcknowledgedEntries() {
        for (int i = 0; i < 100; i++) {
            jobs.enqueue(EnqueueRequest.of("default", "work", null));
        }
        List<Delivery.FromStream> delivered = streams.read("w1", 60, Duration.ZERO, List.of("default"));
        assertThat(delivered).hasSize(60);
        delivered.subList(0, 50).forEach(lifecycle::acknowledge); // 50 acked, 10 still pending, 40 never delivered

        long removed = streams.compact("default");

        assertThat(removed).isEqualTo(50);
        assertThat(redis.opsForStream().size(keys.stream("default"))).isEqualTo(50);
        // Nothing undelivered was dropped: the remaining 40 are still readable.
        assertThat(streams.read("w2", 100, Duration.ZERO, List.of("default"))).hasSize(40);
    }

    @Test
    void compactionKeepsEverythingWhenTheOldestEntryIsStillPending() {
        for (int i = 0; i < 10; i++) {
            jobs.enqueue(EnqueueRequest.of("default", "work", null));
        }
        List<Delivery.FromStream> delivered = streams.read("w1", 10, Duration.ZERO, List.of("default"));
        delivered.subList(1, 10).forEach(lifecycle::acknowledge); // oldest still pending

        assertThat(streams.compact("default")).isZero();
        assertThat(redis.opsForStream().size(keys.stream("default"))).isEqualTo(10);
    }
}
