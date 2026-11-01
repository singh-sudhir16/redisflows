package io.redisflow.events;

import io.redisflow.config.RedisFlowProperties;
import io.redisflow.redis.RedisKeys;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.json.JsonMapper;

/**
 * Publishes worker lifecycle events on the events channel. Job state-change events are
 * published from inside the Lua scripts, atomically with the transition they describe.
 */
public class EventPublisher {

    private static final Logger log = LoggerFactory.getLogger(EventPublisher.class);

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final JsonMapper json;
    private final boolean enabled;

    public EventPublisher(StringRedisTemplate redis, RedisKeys keys, JsonMapper json, RedisFlowProperties props) {
        this.redis = redis;
        this.keys = keys;
        this.json = json;
        this.enabled = props.eventsEnabled();
    }

    public void workerEvent(String type, String workerId) {
        if (!enabled) {
            return;
        }
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("type", type);
        event.put("worker_id", workerId);
        event.put("timestamp", System.currentTimeMillis());
        try {
            redis.convertAndSend(keys.events(), json.writeValueAsString(event));
        } catch (RuntimeException e) {
            log.debug("Could not publish {} event: {}", type, e.getMessage());
        }
    }
}
