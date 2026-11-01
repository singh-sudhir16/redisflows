package io.redisflow.bench;

import java.util.Map;
import java.util.UUID;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

/**
 * Benchmark runner. Each run uses its own key prefix and deletes its keys afterwards, so it can
 * point at a shared Redis without touching other data.
 *
 * <pre>
 *   java -jar redisflow-bench.jar --bench.mode=queue --bench.jobs=50000 --bench.workers=2
 *   java -jar redisflow-bench.jar --bench.mode=http --bench.url=http://localhost:8080 --bench.duration=30s
 * </pre>
 */
@SpringBootApplication
@EnableConfigurationProperties(BenchmarkProperties.class)
public class BenchmarkApplication {

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(BenchmarkApplication.class);
        app.setWebApplicationType(WebApplicationType.NONE);
        app.setDefaultProperties(Map.of("redisflow.key-prefix", "bench-" + UUID.randomUUID().toString().substring(0, 8)));
        System.exit(SpringApplication.exit(app.run(args)));
    }
}
