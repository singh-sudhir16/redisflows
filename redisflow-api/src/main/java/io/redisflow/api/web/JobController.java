package io.redisflow.api.web;

import io.redisflow.job.EnqueueRequest;
import io.redisflow.job.EnqueueResult;
import io.redisflow.job.Job;
import io.redisflow.job.JobService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import java.net.URI;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/jobs")
@Tag(name = "Jobs", description = "Enqueue and inspect jobs")
public class JobController {

    static final String IDEMPOTENCY_KEY = "Idempotency-Key";
    static final String REPLAYED_HEADER = "Idempotent-Replayed";

    private final JobService jobs;

    public JobController(JobService jobs) {
        this.jobs = jobs;
    }

    @PostMapping
    @Operation(summary = "Enqueue a job",
            description = "Returns 202 with the job. Retrying with the same Idempotency-Key and body returns the "
                    + "original job (200, Idempotent-Replayed: true); a different body returns 422.")
    public ResponseEntity<Job> enqueue(
            @Valid @RequestBody EnqueueRequest request,
            @Parameter(description = "Client-chosen key that makes retries safe")
            @RequestHeader(name = IDEMPOTENCY_KEY, required = false) String idempotencyKey) {
        EnqueueResult result = jobs.enqueue(request, idempotencyKey);
        URI location = URI.create("/api/v1/jobs/" + result.jobId());
        if (result.replayed()) {
            return ResponseEntity.ok().location(location).header(REPLAYED_HEADER, "true").body(jobs.get(result.jobId()));
        }
        return ResponseEntity.status(HttpStatus.ACCEPTED).location(location).body(result.job());
    }

    @GetMapping("/{id}")
    @Operation(summary = "Get a job's current state")
    public Job get(@PathVariable String id) {
        return jobs.get(id);
    }
}
