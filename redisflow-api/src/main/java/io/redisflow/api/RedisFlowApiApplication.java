package io.redisflow.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Stateless HTTP front end: enqueue and inspect jobs, locks, analytics, live dashboard. */
@SpringBootApplication
@EnableScheduling
public class RedisFlowApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(RedisFlowApiApplication.class, args);
    }
}
