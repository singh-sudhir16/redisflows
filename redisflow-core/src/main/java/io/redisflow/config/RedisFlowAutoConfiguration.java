package io.redisflow.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.redisflow.analytics.ActivityTracker;
import io.redisflow.analytics.VisitorAnalytics;
import io.redisflow.analytics.WorkerGeoIndex;
import io.redisflow.events.EventPublisher;
import io.redisflow.job.JobService;
import io.redisflow.lock.DistributedLockService;
import io.redisflow.lock.LeaderElection;
import io.redisflow.maintenance.MaintenanceService;
import io.redisflow.queue.DelayedJobDispatcher;
import io.redisflow.queue.PriorityLane;
import io.redisflow.queue.QueueStatsService;
import io.redisflow.queue.StreamQueue;
import io.redisflow.ratelimit.SlidingWindowRateLimiter;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import io.redisflow.retry.BackoffPolicy;
import io.redisflow.worker.JobHandler;
import io.redisflow.worker.JobHandlerRegistry;
import io.redisflow.worker.JobLifecycle;
import io.redisflow.worker.WorkerEngine;
import io.redisflow.worker.WorkerRegistry;
import io.redisflow.worker.WorkerSettings;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.data.redis.autoconfigure.DataRedisAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * Wires the RedisFlow engine into any Spring Boot app that has {@code redisflow-core} on the
 * classpath. Workers and the maintenance leader are opt-in via
 * {@code redisflow.worker.enabled} and {@code redisflow.maintenance.enabled}.
 */
@AutoConfiguration(after = DataRedisAutoConfiguration.class)
@EnableConfigurationProperties(RedisFlowProperties.class)
public class RedisFlowAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RedisKeys redisFlowKeys(RedisFlowProperties props) {
        return new RedisKeys(props.keyPrefix());
    }

    @Bean
    @ConditionalOnMissingBean
    RedisScripts redisFlowScripts() {
        return new RedisScripts();
    }

    @Bean
    @ConditionalOnMissingBean
    StreamQueue streamQueue(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        return new StreamQueue(redis, keys, scripts);
    }

    @Bean
    @ConditionalOnMissingBean
    PriorityLane priorityLane(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        return new PriorityLane(redis, keys, scripts);
    }

    @Bean
    @ConditionalOnMissingBean
    DelayedJobDispatcher delayedJobDispatcher(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts,
                                              StreamQueue streams) {
        return new DelayedJobDispatcher(redis, keys, scripts, streams);
    }

    @Bean
    @ConditionalOnMissingBean
    JobService jobService(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts, StreamQueue streams,
                          RedisFlowProperties props) {
        return new JobService(redis, keys, scripts, streams, props);
    }

    @Bean
    @ConditionalOnMissingBean
    JobLifecycle jobLifecycle(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts,
                              RedisFlowProperties props) {
        return new JobLifecycle(redis, keys, scripts, props);
    }

    @Bean
    @ConditionalOnMissingBean
    QueueStatsService queueStatsService(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts,
                                        PriorityLane lane, DelayedJobDispatcher delayed, RedisFlowProperties props) {
        return new QueueStatsService(redis, keys, scripts, lane, delayed, props);
    }

    @Bean
    @ConditionalOnMissingBean
    BackoffPolicy backoffPolicy(RedisFlowProperties props) {
        RedisFlowProperties.Retry retry = props.retry();
        return new BackoffPolicy(retry.baseDelay(), retry.maxDelay(), retry.jitter());
    }

    @Bean
    @ConditionalOnMissingBean
    DistributedLockService distributedLockService(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        return new DistributedLockService(redis, keys, scripts);
    }

    @Bean
    @ConditionalOnMissingBean
    SlidingWindowRateLimiter slidingWindowRateLimiter(StringRedisTemplate redis, RedisKeys keys,
                                                      RedisScripts scripts, RedisFlowProperties props) {
        return new SlidingWindowRateLimiter(redis, keys, scripts, props.rateLimit().limit(),
                props.rateLimit().window());
    }

    @Bean
    @ConditionalOnMissingBean
    EventPublisher redisFlowEventPublisher(StringRedisTemplate redis, RedisKeys keys, JsonMapper json,
                                           RedisFlowProperties props) {
        return new EventPublisher(redis, keys, json, props);
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerRegistry workerRegistry(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        return new WorkerRegistry(redis, keys, scripts);
    }

    @Bean
    @ConditionalOnMissingBean
    VisitorAnalytics visitorAnalytics(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        return new VisitorAnalytics(redis, keys, scripts);
    }

    @Bean
    @ConditionalOnMissingBean
    ActivityTracker activityTracker(StringRedisTemplate redis, RedisKeys keys) {
        return new ActivityTracker(redis, keys);
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerGeoIndex workerGeoIndex(StringRedisTemplate redis, RedisKeys keys) {
        return new WorkerGeoIndex(redis, keys);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "redisflow.worker", name = "enabled", havingValue = "true")
    static class WorkerConfiguration {

        @Bean
        WorkerSettings workerSettings(RedisFlowProperties props) {
            return WorkerSettings.from(props.worker());
        }

        @Bean
        JobHandlerRegistry jobHandlerRegistry(List<JobHandler> handlers) {
            return new JobHandlerRegistry(handlers);
        }

        @Bean
        WorkerEngine workerEngine(WorkerSettings settings, StreamQueue streams, PriorityLane lane,
                                  JobLifecycle lifecycle, JobHandlerRegistry handlers, BackoffPolicy backoff,
                                  WorkerRegistry registry, EventPublisher events,
                                  ObjectProvider<MeterRegistry> meterRegistry) {
            return new WorkerEngine(settings, streams, lane, lifecycle, handlers, backoff, registry, events,
                    meterRegistry.getIfAvailable(SimpleMeterRegistry::new));
        }
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(prefix = "redisflow.maintenance", name = "enabled", havingValue = "true")
    static class MaintenanceConfiguration {

        @Bean
        MaintenanceService maintenanceService(DistributedLockService locks, DelayedJobDispatcher delayed,
                                              PriorityLane lane, StreamQueue streams, QueueStatsService queues,
                                              RedisFlowProperties props) {
            String candidate = props.worker().resolvedId() + "-maintenance";
            LeaderElection election = new LeaderElection(locks, "maintenance-leader", candidate,
                    props.maintenance().leaderLeaseTtl());
            return new MaintenanceService(election, delayed, lane, streams, queues, props.maintenance());
        }
    }
}
