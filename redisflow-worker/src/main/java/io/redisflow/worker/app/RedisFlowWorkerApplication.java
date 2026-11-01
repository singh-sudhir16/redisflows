package io.redisflow.worker.app;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Worker process. Scale horizontally by running more replicas: each joins the same consumer
 * group with a unique consumer name, and the replicas elect one maintenance leader among them.
 */
@SpringBootApplication
public class RedisFlowWorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisFlowWorkerApplication.class, args);
    }
}
