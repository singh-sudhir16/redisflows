package io.redisflow.queue;

import io.redisflow.redis.RedisKeys;
import java.util.List;

/**
 * A job handed to a worker, plus what is needed to acknowledge it. Stream deliveries are
 * acknowledged with XACK; priority-lane deliveries by releasing their lease.
 */
public sealed interface Delivery {

    String queue();

    String jobId();

    /** Script arguments: [ackKind, ackRef]. */
    String ackKind();

    String ackRef();

    /** Script keys: [ackKey, leaseKey]. */
    List<String> ackKeys(RedisKeys keys);

    record FromStream(String queue, String jobId, String messageId) implements Delivery {
        @Override
        public String ackKind() {
            return "stream";
        }

        @Override
        public String ackRef() {
            return messageId;
        }

        @Override
        public List<String> ackKeys(RedisKeys keys) {
            return List.of(keys.stream(queue), keys.priorityLeases());
        }
    }

    record FromPriorityLane(String queue, String jobId, String member) implements Delivery {
        @Override
        public String ackKind() {
            return "lease";
        }

        @Override
        public String ackRef() {
            return member;
        }

        @Override
        public List<String> ackKeys(RedisKeys keys) {
            return List.of(keys.priorityInflight(), keys.priorityLeases());
        }
    }
}
