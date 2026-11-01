package io.redisflow.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.time.Duration;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

/**
 * All RedisFlow settings, bound from {@code redisflow.*} (e.g. {@code REDISFLOW_WORKER_CONCURRENCY}).
 */
@Validated
@ConfigurationProperties("redisflow")
public record RedisFlowProperties(
        @DefaultValue("redisflow") @NotBlank String keyPrefix,
        @DefaultValue("true") boolean eventsEnabled,
        @DefaultValue @Valid Jobs jobs,
        @DefaultValue @Valid Worker worker,
        @DefaultValue @Valid Retry retry,
        @DefaultValue @Valid Maintenance maintenance,
        @DefaultValue @Valid RateLimit rateLimit,
        @DefaultValue @Valid Locks locks) {

    public record Jobs(
            @DefaultValue("3") @Min(1) @Max(25) int defaultMaxAttempts,
            @DefaultValue("3") @Min(1) @Max(4) int defaultPriority,
            /* Completed jobs expire after this long; dead-lettered jobs are kept for inspection. */
            @DefaultValue("24h") Duration completedRetention,
            @DefaultValue("24h") Duration idempotencyKeyTtl) {
    }

    public record Worker(
            @DefaultValue("false") boolean enabled,
            /* Consumer name inside the group. Must be unique per process; blank = host + pid. */
            @DefaultValue("") String id,
            @DefaultValue({"default", "emails", "payments", "notifications"}) @NotEmpty List<String> queues,
            @DefaultValue("16") @Min(1) @Max(1024) int concurrency,
            @DefaultValue("1s") Duration blockTimeout,
            /* A delivery unacknowledged for this long is assumed lost and is redelivered. */
            @DefaultValue("60s") Duration visibilityTimeout,
            @DefaultValue("5s") Duration reclaimInterval,
            @DefaultValue("30s") Duration jobTimeout,
            @DefaultValue("30s") Duration shutdownTimeout,
            @DefaultValue("5s") Duration heartbeatInterval) {

        public String resolvedId() {
            if (id != null && !id.isBlank()) {
                return id;
            }
            String host;
            try {
                host = InetAddress.getLocalHost().getHostName();
            } catch (Exception e) {
                host = "worker";
            }
            return host + "-" + ManagementFactory.getRuntimeMXBean().getPid();
        }
    }

    public record Retry(
            @DefaultValue("2s") Duration baseDelay,
            @DefaultValue("5m") Duration maxDelay,
            @DefaultValue("true") boolean jitter) {
    }

    public record Maintenance(
            @DefaultValue("false") boolean enabled,
            @DefaultValue("250ms") Duration pollInterval,
            @DefaultValue("500") @Min(1) int batchSize,
            @DefaultValue("15s") Duration leaderLeaseTtl,
            @DefaultValue("30s") Duration compactionInterval) {
    }

    public record RateLimit(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("100") @Min(1) int limit,
            @DefaultValue("1m") Duration window) {
    }

    public record Locks(@DefaultValue("30s") Duration defaultTtl) {
    }
}
