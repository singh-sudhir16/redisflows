package io.redisflow.retry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class BackoffPolicyTest {

    @Test
    void doublesEachAttemptUntilTheCap() {
        BackoffPolicy policy = new BackoffPolicy(Duration.ofSeconds(2), Duration.ofSeconds(30), false);

        assertThat(policy.delayAfter(1)).isEqualTo(Duration.ofSeconds(2));
        assertThat(policy.delayAfter(2)).isEqualTo(Duration.ofSeconds(4));
        assertThat(policy.delayAfter(3)).isEqualTo(Duration.ofSeconds(8));
        assertThat(policy.delayAfter(4)).isEqualTo(Duration.ofSeconds(16));
        assertThat(policy.delayAfter(5)).isEqualTo(Duration.ofSeconds(30));
        assertThat(policy.delayAfter(50)).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void equalJitterStaysBetweenHalfAndFullDelay() {
        BackoffPolicy low = new BackoffPolicy(Duration.ofSeconds(2), Duration.ofMinutes(5), true, () -> 0.0);
        BackoffPolicy high = new BackoffPolicy(Duration.ofSeconds(2), Duration.ofMinutes(5), true, () -> 0.999);

        assertThat(low.delayAfter(3)).isEqualTo(Duration.ofSeconds(4));
        assertThat(high.delayAfter(3)).isBetween(Duration.ofMillis(7990), Duration.ofSeconds(8));
    }

    @Test
    void rejectsInvalidConfiguration() {
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ZERO, Duration.ofSeconds(1), false))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackoffPolicy(Duration.ofSeconds(5), Duration.ofSeconds(1), false))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
