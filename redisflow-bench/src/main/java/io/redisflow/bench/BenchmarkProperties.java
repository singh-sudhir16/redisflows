package io.redisflow.bench;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param mode           {@code queue} (in-process, measures the engine) or {@code http} (load-tests a running API)
 * @param jobs           jobs per queue-mode phase
 * @param producers      concurrent producers (virtual threads)
 * @param workers        worker engines (separate consumers in the group)
 * @param concurrency    job slots per worker
 * @param latencyRate    target enqueue rate (jobs/s) for the steady-state latency phase
 * @param latencyDuration duration of the latency phase
 * @param url            API base URL for http mode
 * @param duration       http mode duration
 * @param httpClients    concurrent HTTP clients
 */
@ConfigurationProperties("bench")
public record BenchmarkProperties(
        @DefaultValue("queue") String mode,
        @DefaultValue("50000") int jobs,
        @DefaultValue("64") int producers,
        @DefaultValue("2") int workers,
        @DefaultValue("64") int concurrency,
        @DefaultValue("2000") int latencyRate,
        @DefaultValue("20s") Duration latencyDuration,
        @DefaultValue("http://localhost:8080") String url,
        @DefaultValue("30s") Duration duration,
        @DefaultValue("64") int httpClients) {
}
