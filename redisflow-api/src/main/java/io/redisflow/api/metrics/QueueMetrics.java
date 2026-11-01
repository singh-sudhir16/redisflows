package io.redisflow.api.metrics;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.queue.QueueStatsService.Overview;
import io.redisflow.queue.QueueStatsService.QueueStats;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Exposes queue depth gauges (backlog, pending, dead letters) for Prometheus alerting, e.g.
 * "backlog growing for 10 minutes" or "dead letters > 0". Sampled every few seconds so a
 * scrape never fans out into Redis calls.
 */
@Component
public class QueueMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);

    private final QueueStatsService stats;
    private final MeterRegistry registry;
    private final Map<String, AtomicLong> gauges = new ConcurrentHashMap<>();

    public QueueMetrics(QueueStatsService stats, MeterRegistry registry) {
        this.stats = stats;
        this.registry = registry;
    }

    @Scheduled(fixedDelayString = "${redisflow.metrics.sample-interval:5s}", initialDelay = 1_000)
    public void sample() {
        try {
            Overview overview = stats.overview();
            for (QueueStats q : overview.queues()) {
                gauge("redisflow.queue.backlog", q.name()).set(Math.max(0, q.backlog()));
                gauge("redisflow.queue.pending", q.name()).set(q.pending());
                gauge("redisflow.queue.dead_letters", q.name()).set(q.deadLetters());
            }
            gauge("redisflow.priority.queued", "priority").set(overview.priorityQueued());
            gauge("redisflow.scheduled", "scheduled").set(overview.scheduled());
            gauge("redisflow.retrying", "retries").set(overview.retrying());
        } catch (RuntimeException e) {
            log.debug("Queue metrics sample failed: {}", e.getMessage());
        }
    }

    private AtomicLong gauge(String name, String queue) {
        return gauges.computeIfAbsent(name + "|" + queue, k -> {
            AtomicLong value = new AtomicLong();
            Gauge.builder(name, value, AtomicLong::get).tag("queue", queue).register(registry);
            return value;
        });
    }
}
