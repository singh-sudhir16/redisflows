package io.redisflow.worker.app.handlers;

import io.redisflow.job.Job;
import io.redisflow.worker.NonRetryableJobException;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/** Payload parsing shared by handlers: a malformed payload will never succeed, so it is not retried. */
final class Payloads {

    private Payloads() {
    }

    static <T> T read(JsonMapper json, Job job, Class<T> type) {
        try {
            return json.readValue(job.payload() == null ? "{}" : job.payload(), type);
        } catch (JacksonException e) {
            throw new NonRetryableJobException("invalid " + job.type() + " payload: " + e.getOriginalMessage());
        }
    }

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new NonRetryableJobException(message);
        }
    }
}
