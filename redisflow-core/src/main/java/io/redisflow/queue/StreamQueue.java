package io.redisflow.queue;

import io.redisflow.queue.Delivery.FromStream;
import io.redisflow.redis.RedisKeys;
import io.redisflow.redis.RedisScripts;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * FIFO queues backed by Redis Streams and a single consumer group.
 *
 * <p>Delivery is at-least-once: a message stays in the group's pending entries list (PEL)
 * until acknowledged, and a message whose consumer goes silent for longer than the visibility
 * timeout is claimed by another worker (XPENDING IDLE + XCLAIM).
 */
public class StreamQueue {

    private final StringRedisTemplate redis;
    private final RedisKeys keys;
    private final RedisScripts scripts;
    private final String streamKeyPrefix;
    private final Set<String> groupsEnsured = ConcurrentHashMap.newKeySet();

    public StreamQueue(StringRedisTemplate redis, RedisKeys keys, RedisScripts scripts) {
        this.redis = redis;
        this.keys = keys;
        this.scripts = scripts;
        this.streamKeyPrefix = keys.stream("");
    }

    /**
     * Creates the consumer group (and stream) if missing. The group starts at id 0, so messages
     * added before any worker existed are still delivered.
     */
    public void ensureGroup(String queue) {
        if (groupsEnsured.contains(queue)) {
            return;
        }
        byte[] key = keys.stream(queue).getBytes(StandardCharsets.UTF_8);
        try {
            redis.execute((RedisCallback<String>) connection -> connection.streamCommands()
                    .xGroupCreate(key, RedisKeys.CONSUMER_GROUP, ReadOffset.from("0-0"), true));
        } catch (DataAccessException e) {
            if (!isBusyGroup(e)) {
                throw e;
            }
        }
        groupsEnsured.add(queue);
    }

    /**
     * Reads never-delivered messages for this consumer. Blocks up to {@code block} when the
     * duration is positive; returns immediately otherwise.
     */
    public List<FromStream> read(String consumer, int count, Duration block, Collection<String> queues) {
        try {
            return readOnce(consumer, count, block, queues);
        } catch (DataAccessException e) {
            if (!isNoGroup(e)) {
                throw e;
            }
            // Redis lost the group (FLUSHALL, failover without persistence): recreate and retry once.
            queues.forEach(groupsEnsured::remove);
            queues.forEach(this::ensureGroup);
            return readOnce(consumer, count, block, queues);
        }
    }

    private List<FromStream> readOnce(String consumer, int count, Duration block, Collection<String> queues) {
        StreamReadOptions options = StreamReadOptions.empty().count(count);
        if (block != null && !block.isZero() && !block.isNegative()) {
            options = options.block(block);
        }
        @SuppressWarnings("unchecked")
        StreamOffset<String>[] offsets = queues.stream()
                .map(q -> StreamOffset.create(keys.stream(q), ReadOffset.lastConsumed()))
                .toArray(StreamOffset[]::new);
        List<MapRecord<String, Object, Object>> records = redis.opsForStream()
                .read(Consumer.from(RedisKeys.CONSUMER_GROUP, consumer), options, offsets);
        return toDeliveries(records);
    }

    /**
     * Claims messages another consumer received but has not acknowledged within {@code minIdle}.
     * XCLAIM re-checks the idle time atomically, so two workers racing for the same message
     * cannot both win.
     */
    public List<FromStream> reclaim(String queue, String consumer, Duration minIdle, int max) {
        String key = keys.stream(queue);
        PendingMessages pending;
        try {
            pending = redis.opsForStream().pending(key, RedisKeys.CONSUMER_GROUP, Range.unbounded(), max, minIdle);
        } catch (DataAccessException e) {
            if (isNoGroup(e)) {
                groupsEnsured.remove(queue);
                ensureGroup(queue);
                return List.of();
            }
            throw e;
        }
        if (pending == null || pending.isEmpty()) {
            return List.of();
        }
        RecordId[] ids = pending.stream().map(PendingMessage::getId).toArray(RecordId[]::new);
        List<MapRecord<String, Object, Object>> claimed = redis.opsForStream()
                .claim(key, RedisKeys.CONSUMER_GROUP, consumer, minIdle, ids);
        return toDeliveries(claimed);
    }

    /** Removes entries every group has already received and acknowledged. */
    public long compact(String queue) {
        Long removed = redis.execute(scripts.streamCompact, List.of(keys.stream(queue)));
        return removed == null ? 0 : removed;
    }

    private List<FromStream> toDeliveries(List<MapRecord<String, Object, Object>> records) {
        if (records == null || records.isEmpty()) {
            return List.of();
        }
        List<FromStream> deliveries = new ArrayList<>(records.size());
        for (MapRecord<String, Object, Object> record : records) {
            String queue = record.getStream().substring(streamKeyPrefix.length());
            Object jobId = record.getValue().get("job_id");
            deliveries.add(new FromStream(queue, jobId == null ? null : jobId.toString(), record.getId().getValue()));
        }
        return deliveries;
    }

    private static boolean isBusyGroup(Throwable e) {
        return hasRedisError(e, "BUSYGROUP");
    }

    private static boolean isNoGroup(Throwable e) {
        return hasRedisError(e, "NOGROUP");
    }

    private static boolean hasRedisError(Throwable e, String code) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(code)) {
                return true;
            }
        }
        return false;
    }
}
