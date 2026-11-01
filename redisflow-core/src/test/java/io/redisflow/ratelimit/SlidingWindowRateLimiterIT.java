package io.redisflow.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;

import io.redisflow.ratelimit.SlidingWindowRateLimiter.Decision;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class SlidingWindowRateLimiterIT extends CoreIntegrationTest {

    @Autowired
    RedisKeys keys;
    @Autowired
    RedisScripts scripts;

    @Test
    void admitsExactlyTheLimitUnderConcurrentLoad() throws Exception {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(redis, keys, scripts, 100,
                Duration.ofSeconds(30));
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Decision>> results = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 300; i++) {
                results.add(pool.submit(() -> {
                    start.await();
                    return limiter.tryAcquire("client-1");
                }));
            }
            start.countDown();
            long allowed = 0;
            for (Future<Decision> r : results) {
                allowed += r.get().allowed() ? 1 : 0;
            }
            assertThat(allowed).isEqualTo(100);
        }
        Decision denied = limiter.tryAcquire("client-1");
        assertThat(denied.allowed()).isFalse();
        assertThat(denied.remaining()).isZero();
        assertThat(denied.retryAfter()).isPositive();
        assertThat(limiter.tryAcquire("client-2").allowed()).isTrue();
    }

    @Test
    void windowSlidesSoCapacityReturns() {
        SlidingWindowRateLimiter limiter = new SlidingWindowRateLimiter(redis, keys, scripts, 3,
                Duration.ofMillis(500));
        for (int i = 0; i < 3; i++) {
            assertThat(limiter.tryAcquire("c").allowed()).isTrue();
        }
        assertThat(limiter.tryAcquire("c").allowed()).isFalse();

        eventually(Duration.ofSeconds(3), () -> limiter.tryAcquire("c").allowed());
    }
}
