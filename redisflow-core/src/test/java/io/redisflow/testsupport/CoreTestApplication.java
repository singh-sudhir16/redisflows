package io.redisflow.testsupport;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

/** Minimal Boot application so core tests run against the real auto-configuration. */
@SpringBootConfiguration
@EnableAutoConfiguration
public class CoreTestApplication {
}
