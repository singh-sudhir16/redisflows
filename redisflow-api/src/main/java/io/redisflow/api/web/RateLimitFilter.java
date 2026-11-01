package io.redisflow.api.web;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.redisflow.ratelimit.SlidingWindowRateLimiter;
import io.redisflow.ratelimit.SlidingWindowRateLimiter.Decision;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Per-client sliding-window rate limit on {@code /api/**}. The client key is the
 * {@code X-User-ID} header when present, otherwise the client IP.
 *
 * <p>Fails open: if Redis is unreachable the request is allowed (and counted), because
 * rejecting all traffic during a cache outage is usually worse than briefly not limiting it.
 */
public class RateLimitFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);

    private final SlidingWindowRateLimiter limiter;
    private final Counter rejected;
    private final Counter failedOpen;

    public RateLimitFilter(SlidingWindowRateLimiter limiter, MeterRegistry meters) {
        this.limiter = limiter;
        this.rejected = Counter.builder("redisflow.ratelimit.rejected").register(meters);
        this.failedOpen = Counter.builder("redisflow.ratelimit.failed_open").register(meters);
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Decision decision;
        try {
            decision = limiter.tryAcquire(clientKey(request));
        } catch (RuntimeException e) {
            failedOpen.increment();
            log.warn("Rate limiter unavailable, allowing request: {}", e.getMessage());
            chain.doFilter(request, response);
            return;
        }

        response.setHeader("X-RateLimit-Limit", Integer.toString(decision.limit()));
        response.setHeader("X-RateLimit-Remaining", Long.toString(decision.remaining()));
        if (decision.allowed()) {
            chain.doFilter(request, response);
            return;
        }

        rejected.increment();
        long retryAfterSeconds = Math.max(1, (decision.retryAfter().toMillis() + 999) / 1000);
        response.setStatus(HttpStatus.TOO_MANY_REQUESTS.value());
        response.setHeader("Retry-After", Long.toString(retryAfterSeconds));
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        response.getWriter().write("{\"type\":\"about:blank\",\"title\":\"Too Many Requests\",\"status\":429,"
                + "\"detail\":\"Rate limit of " + decision.limit() + " requests per " + limiter.window().toSeconds()
                + "s exceeded\"}");
    }

    static String clientKey(HttpServletRequest request) {
        String userId = request.getHeader("X-User-ID");
        if (userId != null && !userId.isBlank() && userId.length() <= 128) {
            return "user:" + userId;
        }
        return "ip:" + request.getRemoteAddr();
    }
}
