package io.redisflow.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.redisflow.job.JobExceptions.IdempotencyConflictException;
import io.redisflow.job.JobExceptions.InvalidJobException;
import io.redisflow.redis.RedisKeys;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import tools.jackson.databind.json.JsonMapper;

class JobServiceIT extends CoreIntegrationTest {

    @Autowired
    JobService jobs;
    @Autowired
    RedisKeys keys;
    @Autowired
    JsonMapper json;

    @Test
    void normalPriorityJobGoesToItsStream() {
        EnqueueResult result = jobs.enqueue(EnqueueRequest.of("emails", "email",
                json.readTree("{\"to\":\"a@example.com\"}")));

        Job stored = jobs.get(result.jobId());
        assertThat(stored.status()).isEqualTo(JobStatus.PENDING);
        assertThat(stored.queue()).isEqualTo("emails");
        assertThat(stored.payload()).isEqualTo("{\"to\":\"a@example.com\"}");
        assertThat(stored.maxAttempts()).isEqualTo(3);
        assertThat(stored.createdAt()).isNotNull();
        assertThat(redis.opsForStream().size(keys.stream("emails"))).isEqualTo(1);
        assertThat(redis.opsForSet().isMember(keys.knownQueues(), "emails")).isTrue();
    }

    @Test
    void highPriorityJobGoesToThePriorityLane() {
        EnqueueResult result = jobs.enqueue(EnqueueRequest.of("payments", "payment", null).withPriority(1));

        assertThat(redis.opsForZSet().score(keys.priority(), RedisKeys.member("payments", result.jobId())))
                .isNotNull();
        assertThat(redis.opsForStream().size(keys.stream("payments"))).isZero();
    }

    @Test
    void futureJobIsScheduledNotQueued() {
        Instant runAt = Instant.now().plus(Duration.ofHours(1));
        EnqueueResult result = jobs.enqueue(EnqueueRequest.of("default", "report", null).withScheduledFor(runAt));

        Job stored = jobs.get(result.jobId());
        assertThat(stored.status()).isEqualTo(JobStatus.SCHEDULED);
        assertThat(stored.scheduledFor().toEpochMilli()).isEqualTo(runAt.toEpochMilli());
        assertThat(redis.opsForZSet().score(keys.scheduled(), RedisKeys.member("default", result.jobId())))
                .isEqualTo((double) runAt.toEpochMilli());
    }

    @Test
    void idempotencyKeyReturnsTheOriginalJobForARetriedRequest() {
        EnqueueRequest request = EnqueueRequest.of("payments", "payment", json.readTree("{\"amount\":10}"));

        EnqueueResult first = jobs.enqueue(request, "order-42");
        EnqueueResult retry = jobs.enqueue(request, "order-42");

        assertThat(first.replayed()).isFalse();
        assertThat(retry.replayed()).isTrue();
        assertThat(retry.jobId()).isEqualTo(first.jobId());
        assertThat(redis.opsForStream().size(keys.stream("payments"))).isEqualTo(1);
    }

    @Test
    void reusingAnIdempotencyKeyWithADifferentBodyIsRejected() {
        jobs.enqueue(EnqueueRequest.of("payments", "payment", json.readTree("{\"amount\":10}")), "order-7");

        assertThatThrownBy(() -> jobs.enqueue(
                EnqueueRequest.of("payments", "payment", json.readTree("{\"amount\":99}")), "order-7"))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    @Test
    void rejectsInvalidInput() {
        assertThatThrownBy(() -> jobs.enqueue(EnqueueRequest.of("bad queue!", "email", null)))
                .isInstanceOf(InvalidJobException.class);
        assertThatThrownBy(() -> jobs.enqueue(EnqueueRequest.of("emails", "email", null).withPriority(9)))
                .isInstanceOf(InvalidJobException.class);
    }
}
