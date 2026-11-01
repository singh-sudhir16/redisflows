package io.redisflow.lock;

import static org.assertj.core.api.Assertions.assertThat;

import io.redisflow.lock.DistributedLockService.LockHandle;
import io.redisflow.testsupport.CoreIntegrationTest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class DistributedLockIT extends CoreIntegrationTest {

    @Autowired
    DistributedLockService locks;

    @Test
    void onlyOneOfManyConcurrentContendersAcquiresTheLock() throws Exception {
        int contenders = 64;
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<LockHandle>>> attempts = new ArrayList<>();
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < contenders; i++) {
                String holder = "node-" + i;
                attempts.add(pool.submit(() -> {
                    start.await();
                    return locks.tryAcquire("invoice-run", holder, Duration.ofSeconds(10));
                }));
            }
            start.countDown();
            int winners = 0;
            for (Future<Optional<LockHandle>> attempt : attempts) {
                winners += attempt.get().isPresent() ? 1 : 0;
            }
            assertThat(winners).isEqualTo(1);
        }
    }

    @Test
    void onlyTheHolderCanReleaseOrRenew() {
        LockHandle handle = locks.tryAcquire("report", "alice", Duration.ofSeconds(10)).orElseThrow();

        assertThat(locks.release("report", "mallory")).isFalse();
        assertThat(locks.renew("report", "mallory", Duration.ofSeconds(10))).isFalse();
        assertThat(locks.inspect("report")).get().extracting(DistributedLockService.LockInfo::holder)
                .isEqualTo("alice");

        assertThat(locks.release(handle)).isTrue();
        assertThat(locks.inspect("report")).isEmpty();
    }

    @Test
    void fencingTokensIncreaseWithEveryAcquisition() {
        LockHandle first = locks.tryAcquire("ledger", "a", Duration.ofSeconds(10)).orElseThrow();
        locks.release(first);
        LockHandle second = locks.tryAcquire("ledger", "b", Duration.ofSeconds(10)).orElseThrow();

        assertThat(second.fencingToken()).isGreaterThan(first.fencingToken());
    }

    @Test
    void expiredLockCanBeTakenOver() {
        locks.tryAcquire("cron", "a", Duration.ofMillis(200)).orElseThrow();
        assertThat(locks.tryAcquire("cron", "b", Duration.ofSeconds(5))).isEmpty();

        eventually(Duration.ofSeconds(3), () -> locks.tryAcquire("cron", "b", Duration.ofSeconds(5)).isPresent());
    }

    @Test
    void leaderElectionFailsOverWhenTheLeaderResigns() {
        LeaderElection a = new LeaderElection(locks, "leader-test", "a", Duration.ofSeconds(5));
        LeaderElection b = new LeaderElection(locks, "leader-test", "b", Duration.ofSeconds(5));

        assertThat(a.tick()).isTrue();
        assertThat(b.tick()).isFalse();
        assertThat(a.tick()).isTrue(); // renewal

        a.resign();

        assertThat(b.tick()).isTrue();
        assertThat(a.tick()).isFalse();
    }
}
