package io.redisflow.api.config;

import io.micrometer.core.instrument.MeterRegistry;
import io.redisflow.analytics.VisitorAnalytics;
import io.redisflow.api.web.RateLimitFilter;
import io.redisflow.api.web.VisitorTrackingFilter;
import io.redisflow.ratelimit.SlidingWindowRateLimiter;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

@Configuration(proxyBeanMethods = false)
public class WebConfig {

    @Bean
    @ConditionalOnProperty(prefix = "redisflow.rate-limit", name = "enabled", havingValue = "true",
            matchIfMissing = true)
    FilterRegistrationBean<RateLimitFilter> rateLimitFilter(SlidingWindowRateLimiter limiter, MeterRegistry meters) {
        FilterRegistrationBean<RateLimitFilter> registration =
                new FilterRegistrationBean<>(new RateLimitFilter(limiter, meters));
        registration.addUrlPatterns("/api/*");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 10);
        return registration;
    }

    @Bean
    FilterRegistrationBean<VisitorTrackingFilter> visitorTrackingFilter(VisitorAnalytics analytics) {
        FilterRegistrationBean<VisitorTrackingFilter> registration =
                new FilterRegistrationBean<>(new VisitorTrackingFilter(analytics));
        registration.addUrlPatterns("/api/*");
        registration.setOrder(Ordered.HIGHEST_PRECEDENCE + 20);
        return registration;
    }

    @Bean
    OpenAPI redisFlowOpenApi() {
        return new OpenAPI().info(new Info()
                .title("RedisFlow API")
                .version("v1")
                .description("Distributed job queue on Redis: reliable delivery, retries with backoff, "
                        + "dead-letter queues, priorities, scheduling, locks and rate limiting."));
    }
}
