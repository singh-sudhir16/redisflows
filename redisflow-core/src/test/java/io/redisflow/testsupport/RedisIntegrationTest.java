package io.redisflow.testsupport;

import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.redisflow.events.EventPublisher;
import io.redisflow.queue.PriorityLane;
import io.redisflow.queue.StreamQueue;
import io.redisflow.retry.BackoffPolicy;
import io.redisflow.worker.JobHandler;
import io.redisflow.worker.JobHandlerRegistry;
import io.redisflow.worker.JobLifecycle;
import io.redisflow.worker.WorkerEngine;
import io.redisflow.worker.WorkerRegistry;
import io.redisflow.worker.WorkerSettings;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for tests that need a real Redis. One container is started per JVM (singleton
 * container pattern) and the database is flushed before every test.
 */
public abstract class RedisIntegrationTest {

    @SuppressWarnings("resource")
    protected static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7.4-alpine")).withExposedPorts(6379);

    static {
        REDIS.start();
    }

    @DynamicPropertySource
    static void redisProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired
    protected StringRedisTemplate redis;
    @Autowired
    protected StreamQueue streams;
    @Autowired
    protected PriorityLane priorityLane;
    @Autowired
    protected JobLifecycle lifecycle;
    @Autowired
    protected WorkerRegistry workerRegistry;
    @Autowired
    protected EventPublisher events;

    private final List<WorkerEngine> engines = new ArrayList<>();

    @BeforeEach
    void flushRedis() {
        redis.execute((org.springframework.data.redis.core.RedisCallback<Object>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
    }

    @AfterEach
    void stopEngines() {
        engines.forEach(WorkerEngine::stop);
        engines.clear();
    }

    /** Worker settings tuned for fast tests: short block, visibility and reclaim intervals. */
    protected static WorkerSettings fastSettings(String id, int concurrency, List<String> queues) {
        return new WorkerSettings(id, queues, concurrency, Duration.ofMillis(200), Duration.ofSeconds(2),
                Duration.ofMillis(300), Duration.ofSeconds(1), Duration.ofSeconds(5), Duration.ofSeconds(1));
    }

    protected WorkerEngine startEngine(WorkerSettings settings, JobHandler... handlers) {
        WorkerEngine engine = new WorkerEngine(settings, streams, priorityLane, lifecycle,
                new JobHandlerRegistry(List.of(handlers)),
                new BackoffPolicy(Duration.ofMillis(50), Duration.ofMillis(100), false),
                workerRegistry, events, new SimpleMeterRegistry());
        engine.start();
        engines.add(engine);
        return engine;
    }

    protected static void eventually(Duration timeout, BooleanSupplier condition) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(25)).until(condition::getAsBoolean);
    }
}
