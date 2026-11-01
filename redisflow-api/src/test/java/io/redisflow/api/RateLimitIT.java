package io.redisflow.api;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.redisflow.testsupport.RedisIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;

@SpringBootTest(properties = {"redisflow.rate-limit.limit=5", "redisflow.rate-limit.window=1m",
        "redisflow.key-prefix=ratetest"})
@AutoConfigureMockMvc
class RateLimitIT extends RedisIntegrationTest {

    @Autowired
    MockMvc mvc;

    @Test
    void sixthRequestInTheWindowGets429WithRetryAfter() throws Exception {
        for (int i = 0; i < 5; i++) {
            mvc.perform(get("/api/v1/stats").header("X-User-ID", "alice"))
                    .andExpect(status().isOk())
                    .andExpect(header().string("X-RateLimit-Remaining", String.valueOf(4 - i)));
        }

        mvc.perform(get("/api/v1/stats").header("X-User-ID", "alice"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().exists("Retry-After"));

        // Limits are per client.
        mvc.perform(get("/api/v1/stats").header("X-User-ID", "bob")).andExpect(status().isOk());
    }

    @Test
    void actuatorIsNotRateLimited() throws Exception {
        for (int i = 0; i < 10; i++) {
            mvc.perform(get("/actuator/health")).andExpect(status().isOk());
        }
    }
}
