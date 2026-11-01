package io.redisflow.bench;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.redisflow.events.EventPublisher;
import io.redisflow.job.EnqueueRequest;
import io.redisflow.job.JobService;
import io.redisflow.queue.PriorityLane;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.queue.StreamQueue;
import io.redisflow.redis.RedisKeys;
import io.redisflow.retry.BackoffPolicy;
import io.redisflow.worker.JobHandler;
import io.redisflow.worker.JobHandlerRegistry;
import io.redisflow.worker.JobLifecycle;
import io.redisflow.worker.WorkerEngine;
import io.redisflow.worker.WorkerRegistry;
import io.redisflow.worker.WorkerSettings;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

@Component
class BenchmarkRunner implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(BenchmarkRunner.class);

    private final BenchmarkProperties bench;
    private final JobService jobs;
    private final QueueStatsService stats;
    private final StreamQueue streams;
    private final PriorityLane priorityLane;
    private final JobLifecycle lifecycle;
    private final WorkerRegistry workerRegistry;
    private final EventPublisher events;
    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final List<String> report = new ArrayList<>();

    BenchmarkRunner(BenchmarkProperties bench, JobService jobs, QueueStatsService stats, StreamQueue streams,
                    PriorityLane priorityLane, JobLifecycle lifecycle, WorkerRegistry workerRegistry,
                    EventPublisher events, StringRedisTemplate redis, RedisKeys keys) {
        this.bench = bench;
        this.jobs = jobs;
        this.stats = stats;
        this.streams = streams;
        this.priorityLane = priorityLane;
        this.lifecycle = lifecycle;
        this.workerRegistry = workerRegistry;
        this.events = events;
        this.redis = redis;
        this.keys = keys;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String redisVersion = redis.execute((RedisCallback<String>) c ->
                String.valueOf(c.serverCommands().info("server").getProperty("redis_version")));
        log.info("RedisFlow benchmark: mode={} redis={} cpus={} java={}", bench.mode(), redisVersion,
                Runtime.getRuntime().availableProcessors(), Runtime.version());
        try {
            if ("http".equalsIgnoreCase(bench.mode())) {
                httpLoad();
            } else {
                enqueueThroughput();
                drainThroughput();
                endToEndLatency();
            }
        } finally {
            cleanup();
        }
        System.out.println();
        System.out.println("| Scenario | Result |");
        System.out.println("|---|---|");
        report.forEach(System.out::println);
        System.out.println();
    }

    /** Phase 1: how fast producers can enqueue (one atomic Lua script per job). */
    private void enqueueThroughput() throws InterruptedException {
        int total = bench.jobs();
        Latencies latencies = new Latencies(total);
        AtomicInteger next = new AtomicInteger();
        long start = System.nanoTime();
        try (ExecutorService producers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int p = 0; p < bench.producers(); p++) {
                producers.execute(() -> {
                    while (next.getAndIncrement() < total) {
                        long t0 = System.nanoTime();
                        jobs.enqueue(EnqueueRequest.of("bench", "noop", null));
                        latencies.add((System.nanoTime() - t0) / 1_000);
                    }
                });
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        double[] p = latencies.percentilesMillis();
        row("Enqueue throughput (%d producers)".formatted(bench.producers()),
                "%,.0f jobs/s — p50 %.2f ms, p99 %.2f ms".formatted(total / seconds, p[0], p[2]));
    }

    /** Phase 2: how fast workers drain an existing backlog (claim + run no-op + complete/ack). */
    private void drainThroughput() {
        long target = bench.jobs();
        long start = System.nanoTime();
        List<WorkerEngine> engines = startEngines();
        waitUntil(() -> stats.totals().completed() >= target, Duration.ofMinutes(10));
        double seconds = (System.nanoTime() - start) / 1e9;
        engines.forEach(WorkerEngine::stop);
        row("Drain %,d-job backlog (%d workers x %d slots)".formatted(target, bench.workers(), bench.concurrency()),
                "%,.0f jobs/s".formatted(target / seconds));
    }

    /** Phase 3: enqueue-to-completion latency at a steady arrival rate, measured on the Redis clock. */
    private void endToEndLatency() throws InterruptedException {
        List<WorkerEngine> engines = startEngines();
        long before = stats.totals().completed();
        int rate = bench.latencyRate();
        long total = rate * bench.latencyDuration().toSeconds();
        List<String> ids = java.util.Collections.synchronizedList(new ArrayList<>((int) total));
        long intervalNanos = 1_000_000_000L * bench.producers() / rate;
        AtomicLong sent = new AtomicLong();
        long start = System.nanoTime();
        try (ExecutorService producers = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int p = 0; p < bench.producers(); p++) {
                long offset = intervalNanos * p / bench.producers();
                producers.execute(() -> {
                    long nextAt = start + offset;
                    while (sent.getAndIncrement() < total) {
                        long wait = nextAt - System.nanoTime();
                        if (wait > 0) {
                            LockSupport.parkNanos(wait);
                        }
                        ids.add(jobs.enqueue(EnqueueRequest.of("bench", "noop", null)).jobId());
                        nextAt += intervalNanos;
                    }
                });
            }
        }
        waitUntil(() -> stats.totals().completed() - before >= ids.size(), Duration.ofMinutes(5));
        engines.forEach(WorkerEngine::stop);

        Latencies latencies = new Latencies(ids.size());
        for (int i = 0; i < ids.size(); i += 1_000) {
            List<String> chunk = ids.subList(i, Math.min(ids.size(), i + 1_000));
            List<Object> replies = redis.executePipelined((RedisCallback<Object>) c -> {
                for (String id : chunk) {
                    c.hashCommands().hMGet(keys.job(id).getBytes(StandardCharsets.UTF_8),
                            "created_at".getBytes(StandardCharsets.UTF_8),
                            "completed_at".getBytes(StandardCharsets.UTF_8));
                }
                return null;
            });
            for (Object reply : replies) {
                List<?> fields = (List<?>) reply;
                if (fields.get(0) != null && fields.get(1) != null) {
                    long created = Long.parseLong(fields.get(0).toString());
                    long completed = Long.parseLong(fields.get(1).toString());
                    latencies.add((completed - created) * 1_000);
                }
            }
        }
        double[] p = latencies.percentilesMillis();
        row("End-to-end latency at %,d jobs/s (%d samples)".formatted(rate, latencies.count()),
                "p50 %.0f ms, p95 %.0f ms, p99 %.0f ms (Redis clock, 1 ms resolution)".formatted(p[0], p[1], p[2]));
    }

    /** HTTP mode: load-test POST /api/v1/jobs on a running API (start it with rate limiting disabled). */
    private void httpLoad() throws InterruptedException {
        HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1)
                .executor(Executors.newVirtualThreadPerTaskExecutor()).build();
        URI uri = URI.create(bench.url() + "/api/v1/jobs");
        String body = "{\"queue\":\"bench\",\"type\":\"noop\",\"payload\":{\"n\":1}}";
        Latencies latencies = new Latencies(5_000_000);
        AtomicLong ok = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        long deadline = System.nanoTime() + bench.duration().toNanos();
        long start = System.nanoTime();
        try (ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < bench.httpClients(); i++) {
                clients.execute(() -> {
                    HttpRequest request = HttpRequest.newBuilder(uri).header("Content-Type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString(body)).build();
                    while (System.nanoTime() < deadline) {
                        long t0 = System.nanoTime();
                        try {
                            HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                            if (response.statusCode() == 202) {
                                ok.incrementAndGet();
                                latencies.add((System.nanoTime() - t0) / 1_000);
                            } else {
                                errors.incrementAndGet();
                            }
                        } catch (Exception e) {
                            errors.incrementAndGet();
                        }
                    }
                });
            }
        }
        double seconds = (System.nanoTime() - start) / 1e9;
        double[] p = latencies.percentilesMillis();
        row("HTTP POST /api/v1/jobs (%d clients, %ds)".formatted(bench.httpClients(), bench.duration().toSeconds()),
                "%,.0f req/s — p50 %.2f ms, p95 %.2f ms, p99 %.2f ms, errors %d".formatted(
                        ok.get() / seconds, p[0], p[1], p[2], errors.get()));
    }

    private List<WorkerEngine> startEngines() {
        JobHandler noop = new JobHandler() {
            @Override
            public String type() {
                return "noop";
            }

            @Override
            public void handle(io.redisflow.job.Job job) {
            }
        };
        List<WorkerEngine> engines = new ArrayList<>();
        for (int w = 0; w < bench.workers(); w++) {
            WorkerSettings settings = new WorkerSettings("bench-worker-" + w, List.of("bench"), bench.concurrency(),
                    Duration.ofMillis(500), Duration.ofSeconds(60), Duration.ofSeconds(5), Duration.ofSeconds(30),
                    Duration.ofSeconds(30), Duration.ofSeconds(5));
            WorkerEngine engine = new WorkerEngine(settings, streams, priorityLane, lifecycle,
                    new JobHandlerRegistry(List.of(noop)),
                    new BackoffPolicy(Duration.ofSeconds(1), Duration.ofSeconds(10), true), workerRegistry, events,
                    new SimpleMeterRegistry());
            engine.start();
            engines.add(engine);
        }
        return engines;
    }

    private void cleanup() {
        String pattern = keys.prefix() + "*";
        long deleted = redis.execute((RedisCallback<Long>) c -> {
            long count = 0;
            List<byte[]> batch = new ArrayList<>();
            try (Cursor<byte[]> cursor = c.keyCommands().scan(ScanOptions.scanOptions().match(pattern).count(1000)
                    .build())) {
                while (cursor.hasNext()) {
                    batch.add(cursor.next());
                    if (batch.size() == 1000) {
                        count += unlink(c, batch);
                    }
                }
            }
            return count + unlink(c, batch);
        });
        log.info("Removed {} benchmark keys ({})", deleted, pattern);
    }

    private static long unlink(RedisConnection c, List<byte[]> batch) {
        if (batch.isEmpty()) {
            return 0;
        }
        Long n = c.keyCommands().unlink(batch.toArray(byte[][]::new));
        batch.clear();
        return n == null ? 0 : n;
    }

    private void row(String scenario, String result) {
        String line = "| " + scenario + " | " + result + " |";
        report.add(line);
        log.info(String.format(Locale.ROOT, "%s -> %s", scenario, result));
    }

    private static void waitUntil(java.util.function.BooleanSupplier condition, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("benchmark did not finish within " + timeout);
            }
            LockSupport.parkNanos(Duration.ofMillis(20).toNanos());
        }
    }
}
