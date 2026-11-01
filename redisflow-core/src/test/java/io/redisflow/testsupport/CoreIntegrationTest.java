package io.redisflow.testsupport;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

/** Core-module integration tests: full auto-configuration against a real Redis container. */
@SpringBootTest(classes = CoreTestApplication.class)
@ActiveProfiles("core-test")
public abstract class CoreIntegrationTest extends RedisIntegrationTest {
}
