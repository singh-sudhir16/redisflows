package io.redisflow.job;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Converts between the Redis hash representation of a job and {@link Job}. */
public final class JobCodec {

    private JobCodec() {
    }

    /** Builds a job from a flat {@code [field, value, field, value, ...]} list (HGETALL / script reply). */
    public static Job fromFlatList(List<?> flat) {
        Map<String, String> fields = new HashMap<>(flat.size() / 2 + 1);
        for (int i = 0; i + 1 < flat.size(); i += 2) {
            fields.put(String.valueOf(flat.get(i)), String.valueOf(flat.get(i + 1)));
        }
        return fromMap(fields);
    }

    public static Job fromMap(Map<String, String> f) {
        return new Job(
                f.get("id"),
                f.get("queue"),
                f.get("type"),
                blankToNull(f.get("payload")),
                JobStatus.fromValue(f.getOrDefault("status", "pending")),
                intValue(f.get("priority"), Priority.NORMAL),
                intValue(f.get("attempts"), 0),
                intValue(f.get("max_attempts"), 1),
                blankToNull(f.get("worker_id")),
                instant(f.get("created_at")),
                instant(f.get("scheduled_for")),
                instant(f.get("started_at")),
                instant(f.get("completed_at")),
                instant(f.get("next_retry_at")),
                blankToNull(f.get("last_error")));
    }

    private static int intValue(String value, int fallback) {
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value);
    }

    private static Instant instant(String epochMillis) {
        if (epochMillis == null || epochMillis.isBlank()) {
            return null;
        }
        return Instant.ofEpochMilli(Long.parseLong(epochMillis));
    }

    private static String blankToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}
