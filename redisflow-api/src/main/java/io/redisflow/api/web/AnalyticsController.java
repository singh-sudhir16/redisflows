package io.redisflow.api.web;

import io.redisflow.analytics.ActivityTracker;
import io.redisflow.analytics.VisitorAnalytics;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.time.LocalDate;
import java.util.Map;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Analytics", description = "HyperLogLog unique counts and bitmap daily-active users")
public class AnalyticsController {

    private final VisitorAnalytics visitors;
    private final ActivityTracker activity;

    public AnalyticsController(VisitorAnalytics visitors, ActivityTracker activity) {
        this.visitors = visitors;
        this.activity = activity;
    }

    @GetMapping("/analytics/visitors")
    @Operation(summary = "Approximate unique visitors (X-User-ID) and client IPs over a date range (UTC)")
    public Map<String, Object> uniqueVisitors(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate to) {
        LocalDate end = to == null ? ActivityTracker.today() : to;
        LocalDate start = from == null ? end : from;
        return Map.of("from", start.toString(), "to", end.toString(),
                "unique_visitors", visitors.uniqueVisitors(start, end),
                "unique_ips", visitors.uniqueIps(start, end));
    }

    @PostMapping("/activity/{userId}")
    @Operation(summary = "Mark a numeric user id active today")
    public Map<String, Object> markActive(@PathVariable long userId) {
        activity.markActive(userId);
        return Map.of("user_id", userId, "active_today", true);
    }

    @GetMapping("/activity/{userId}")
    public Map<String, Object> isActive(@PathVariable long userId) {
        return Map.of("user_id", userId, "active_today", activity.isActive(userId, ActivityTracker.today()));
    }

    @GetMapping("/activity/count")
    @Operation(summary = "Daily active users for a date (default today)")
    public Map<String, Object> activeCount(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {
        LocalDate day = date == null ? ActivityTracker.today() : date;
        return Map.of("date", day.toString(), "active_users", activity.activeCount(day));
    }
}
