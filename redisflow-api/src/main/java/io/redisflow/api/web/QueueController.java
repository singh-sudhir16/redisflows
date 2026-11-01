package io.redisflow.api.web;

import io.redisflow.job.EnqueueRequest;
import io.redisflow.job.Job;
import io.redisflow.job.JobService;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.queue.QueueStatsService.Overview;
import io.redisflow.queue.QueueStatsService.QueueStats;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Pattern;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
@Tag(name = "Queues", description = "Queue health, dead-letter inspection and replay")
public class QueueController {

    private final QueueStatsService stats;
    private final JobService jobs;

    public QueueController(QueueStatsService stats, JobService jobs) {
        this.stats = stats;
        this.jobs = jobs;
    }

    @GetMapping("/stats")
    @Operation(summary = "System overview: every queue, priority lane, scheduled/retrying jobs and totals")
    public Overview overview() {
        return stats.overview();
    }

    @GetMapping("/queues")
    @Operation(summary = "List queues with backlog, pending and dead-letter counts")
    public Map<String, List<QueueStats>> queues() {
        return Map.of("queues", stats.knownQueues().stream().map(stats::queue).toList());
    }

    @GetMapping("/queues/{queue}")
    public QueueStats queue(@PathVariable @Pattern(regexp = EnqueueRequest.QUEUE_PATTERN) String queue) {
        return stats.queue(queue);
    }

    @GetMapping("/queues/{queue}/dlq")
    @Operation(summary = "Dead-lettered jobs, newest first")
    public Map<String, Object> deadLetters(
            @PathVariable @Pattern(regexp = EnqueueRequest.QUEUE_PATTERN) String queue,
            @RequestParam(defaultValue = "50") @Min(1) @Max(500) int limit) {
        List<Job> items = jobs.deadLetters(queue, limit).stream()
                .map(jobs::find)
                .flatMap(java.util.Optional::stream)
                .toList();
        return Map.of("queue", queue, "count", stats.queue(queue).deadLetters(), "items", items);
    }

    @PostMapping("/queues/{queue}/dlq/{jobId}/replay")
    @Operation(summary = "Re-enqueue a dead-lettered job with a fresh attempt budget")
    public Job replay(@PathVariable @Pattern(regexp = EnqueueRequest.QUEUE_PATTERN) String queue,
                      @PathVariable String jobId) {
        return jobs.replayDeadLetter(queue, jobId);
    }
}
