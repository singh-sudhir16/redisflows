package io.redisflow.api.realtime;

import io.redisflow.redis.RedisKeys;
import java.nio.charset.StandardCharsets;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;

/**
 * Real-time path: workers publish lifecycle events to a Redis Pub/Sub channel (from inside the
 * state-transition Lua scripts); every API replica subscribes and pushes them to its WebSocket
 * clients. Pub/Sub is fire-and-forget, which suits a live view; job state itself never depends on it.
 */
@Configuration(proxyBeanMethods = false)
@EnableWebSocket
public class RealtimeConfig implements WebSocketConfigurer {

    private final EventStreamHandler handler = new EventStreamHandler();

    @Bean
    EventStreamHandler eventStreamHandler() {
        return handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws/events").setAllowedOriginPatterns("*");
    }

    @Bean
    RedisMessageListenerContainer redisEventListener(RedisConnectionFactory connectionFactory, RedisKeys keys) {
        RedisMessageListenerContainer container = new RedisMessageListenerContainer();
        container.setConnectionFactory(connectionFactory);
        container.addMessageListener(
                (message, pattern) -> handler.broadcast(new String(message.getBody(), StandardCharsets.UTF_8)),
                new ChannelTopic(keys.events()));
        return container;
    }
}
