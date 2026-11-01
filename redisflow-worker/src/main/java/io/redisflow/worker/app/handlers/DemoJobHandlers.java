package io.redisflow.worker.app.handlers;

import io.redisflow.job.Job;
import io.redisflow.worker.JobHandler;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

/** Small handlers for demos and failure testing. */
@Configuration(proxyBeanMethods = false)
public class DemoJobHandlers {

    private static final Logger log = LoggerFactory.getLogger(DemoJobHandlers.class);

    record NotificationPayload(String userId, String title) {
    }

    record FlakyPayload(Double failureRate) {
    }

    record SlowPayload(Double seconds) {
    }

    /** Push notification (simulated, ~50 ms). */
    @Bean
    JobHandler notificationHandler(JsonMapper json) {
        return handler("notification", job -> {
            NotificationPayload n = Payloads.read(json, job, NotificationPayload.class);
            Thread.sleep(50);
            log.info("[notification] user={} title={}", n.userId(), n.title());
        });
    }

    /** Fails with probability {@code failure_rate} (default 0.5): exercises retries and the DLQ. */
    @Bean
    JobHandler flakyHandler(JsonMapper json) {
        return handler("flaky", job -> {
            FlakyPayload p = Payloads.read(json, job, FlakyPayload.class);
            double rate = p.failureRate() == null ? 0.5 : p.failureRate();
            if (ThreadLocalRandom.current().nextDouble() < rate) {
                throw new IllegalStateException("simulated transient failure on attempt " + job.attempts());
            }
        });
    }

    /** Sleeps {@code seconds} (default 5): exercises timeouts, graceful shutdown and crash recovery. */
    @Bean
    JobHandler slowHandler(JsonMapper json) {
        return handler("slow", job -> {
            SlowPayload p = Payloads.read(json, job, SlowPayload.class);
            Thread.sleep(Duration.ofMillis((long) ((p.seconds() == null ? 5 : p.seconds()) * 1000)));
        });
    }

    /** Does nothing: measures queue overhead in benchmarks. */
    @Bean
    JobHandler noopHandler() {
        return handler("noop", job -> { });
    }

    interface Body {
        void run(Job job) throws Exception;
    }

    private static JobHandler handler(String type, Body body) {
        return new JobHandler() {
            @Override
            public String type() {
                return type;
            }

            @Override
            public void handle(Job job) throws Exception {
                body.run(job);
            }
        };
    }
}
