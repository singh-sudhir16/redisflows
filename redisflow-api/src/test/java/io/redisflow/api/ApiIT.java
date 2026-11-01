package io.redisflow.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import io.redisflow.testsupport.RedisIntegrationTest;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.handler.TextWebSocketHandler;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {"redisflow.rate-limit.limit=10000", "redisflow.key-prefix=apitest"})
@AutoConfigureMockMvc
class ApiIT extends RedisIntegrationTest {

    @Autowired
    MockMvc mvc;

    @LocalServerPort
    int port;

    @Test
    void enqueueReturns202WithLocationAndSnakeCaseJson() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"emails\",\"type\":\"email\",\"max_attempts\":5,"
                                + "\"payload\":{\"to\":\"a@example.com\"}}"))
                .andExpect(status().isAccepted())
                .andExpect(header().string("Location", containsString("/api/v1/jobs/")))
                .andExpect(jsonPath("$.status").value("pending"))
                .andExpect(jsonPath("$.max_attempts").value(5))
                .andExpect(jsonPath("$.payload.to").value("a@example.com"));
    }

    @Test
    void jobCanBeFetchedById() throws Exception {
        MvcResult created = mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"type\":\"report\"}"))
                .andExpect(status().isAccepted()).andReturn();
        String location = created.getResponse().getHeader("Location");

        mvc.perform(get(location))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queue").value("default"))
                .andExpect(jsonPath("$.type").value("report"));
    }

    @Test
    void invalidRequestReturnsProblemDetail() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"bad queue\",\"type\":\"\",\"priority\":9}"))
                .andExpect(status().isBadRequest())
                .andExpect(header().string("Content-Type", containsString("application/problem+json")))
                .andExpect(jsonPath("$.status").value(400));
    }

    @Test
    void unknownJobReturns404() throws Exception {
        mvc.perform(get("/api/v1/jobs/does-not-exist"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.title").value("Job not found"));
    }

    @Test
    void idempotencyKeyReplaysAndRejectsMismatchedBodies() throws Exception {
        String body = "{\"queue\":\"payments\",\"type\":\"payment\",\"payload\":{\"amount\":10}}";
        MvcResult first = mvc.perform(post("/api/v1/jobs").header("Idempotency-Key", "order-1")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isAccepted()).andReturn();
        String id = com.jayway.jsonpath.JsonPath.read(first.getResponse().getContentAsString(), "$.id");

        mvc.perform(post("/api/v1/jobs").header("Idempotency-Key", "order-1")
                        .contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isOk())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andExpect(jsonPath("$.id").value(id));

        mvc.perform(post("/api/v1/jobs").header("Idempotency-Key", "order-1")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"queue\":\"payments\",\"type\":\"payment\",\"payload\":{\"amount\":99}}"))
                .andExpect(status().isUnprocessableContent())
                .andExpect(jsonPath("$.existing_job_id").value(id));
    }

    @Test
    void locksAreExclusiveAndCarryFencingTokens() throws Exception {
        mvc.perform(post("/api/v1/locks/nightly-report").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"holder\":\"node-a\",\"ttl_ms\":10000}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.acquired").value(true))
                .andExpect(jsonPath("$.fencing_token").value(1));

        mvc.perform(post("/api/v1/locks/nightly-report").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"holder\":\"node-b\"}"))
                .andExpect(status().isConflict());

        mvc.perform(org.springframework.test.web.servlet.request.MockMvcRequestBuilders
                        .delete("/api/v1/locks/nightly-report").param("holder", "node-b"))
                .andExpect(status().isConflict());
    }

    @Test
    void statsListQueuesThatReceivedJobs() throws Exception {
        mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                .content("{\"queue\":\"reports\",\"type\":\"report\"}")).andExpect(status().isAccepted());

        mvc.perform(get("/api/v1/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.queues[*].name", hasItem("reports")))
                .andExpect(jsonPath("$.totals.enqueued").value(1));
    }

    @Test
    void healthAndPrometheusEndpointsAreExposed() throws Exception {
        mvc.perform(get("/actuator/health")).andExpect(status().isOk()).andExpect(jsonPath("$.status").value("UP"));
        mvc.perform(get("/actuator/prometheus")).andExpect(status().isOk());
    }

    /**
     * Concurrency smoke test for the request path. Request-path Redis calls must use the shared
     * multiplexed connection, never a pooled dedicated one: under sustained load a client-side
     * pipeline in a filter once exhausted the pool and stalled the server (see README).
     */
    @Test
    void handlesManyConcurrentRequestsWithoutStalling() throws Exception {
        java.net.http.HttpClient client = java.net.http.HttpClient.newBuilder()
                .version(java.net.http.HttpClient.Version.HTTP_1_1).build();
        java.net.http.HttpRequest request = java.net.http.HttpRequest
                .newBuilder(URI.create("http://localhost:" + port + "/api/v1/jobs"))
                .timeout(java.time.Duration.ofSeconds(10))
                .header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString("{\"type\":\"noop\"}"))
                .build();
        java.util.List<java.util.concurrent.Future<Integer>> responses = new java.util.ArrayList<>();
        try (var pool = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 256; i++) {
                responses.add(pool.submit(() ->
                        client.send(request, java.net.http.HttpResponse.BodyHandlers.discarding()).statusCode()));
            }
            for (var response : responses) {
                assertThat(response.get(20, TimeUnit.SECONDS)).isEqualTo(202);
            }
        }
    }

    @Test
    void lifecycleEventsAreStreamedOverWebSocket() throws Exception {
        BlockingQueue<String> received = new LinkedBlockingQueue<>();
        WebSocketSession session = new StandardWebSocketClient()
                .execute(new TextWebSocketHandler() {
                    @Override
                    protected void handleTextMessage(WebSocketSession s, TextMessage message) {
                        received.add(message.getPayload());
                    }
                }, null, URI.create("ws://localhost:" + port + "/ws/events"))
                .get(5, TimeUnit.SECONDS);
        try {
            Thread.sleep(300); // let the Redis subscription settle
            mvc.perform(post("/api/v1/jobs").contentType(MediaType.APPLICATION_JSON)
                    .content("{\"type\":\"email\",\"queue\":\"emails\"}")).andExpect(status().isAccepted());

            String event = received.poll(5, TimeUnit.SECONDS);
            assertThat(event).contains("\"type\":\"job.enqueued\"").contains("\"queue\":\"emails\"");
        } finally {
            session.close();
        }
    }
}
