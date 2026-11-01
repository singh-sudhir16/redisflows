package io.redisflow.api.web;

import io.redisflow.analytics.ActivityTracker;
import io.redisflow.analytics.VisitorAnalytics;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.worker.WorkerRegistry;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.Duration;
import java.time.LocalDate;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

/** Everything the dashboard shows, in one request (one poll instead of five). */
@RestController
@Tag(name = "Dashboard")
public class DashboardSummaryController {

    private final QueueStatsService stats;
    private final WorkerRegistry workers;
    private final VisitorAnalytics visitors;
    private final ActivityTracker activity;

    public DashboardSummaryController(QueueStatsService stats, WorkerRegistry workers, VisitorAnalytics visitors,
                                      ActivityTracker activity) {
        this.stats = stats;
        this.workers = workers;
        this.visitors = visitors;
        this.activity = activity;
    }

    @GetMapping("/api/v1/dashboard/summary")
    @Operation(summary = "Queue overview, live workers and today's analytics")
    public Map<String, Object> summary() {
        LocalDate today = ActivityTracker.today();
        return Map.of(
                "overview", stats.overview(),
                "workers", workers.live(Duration.ofSeconds(30)),
                "unique_visitors_today", visitors.uniqueVisitors(today, today),
                "active_users_today", activity.activeCount(today));
    }
}
