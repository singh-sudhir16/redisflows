package io.redisflow.api.web;

import io.redisflow.analytics.VisitorAnalytics;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.filter.OncePerRequestFilter;

/** Feeds the HyperLogLog unique-visitor counters from API traffic (best effort, never blocks a request). */
public class VisitorTrackingFilter extends OncePerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(VisitorTrackingFilter.class);

    private final VisitorAnalytics analytics;

    public VisitorTrackingFilter(VisitorAnalytics analytics) {
        this.analytics = analytics;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        try {
            analytics.track(request.getHeader("X-User-ID"), request.getRemoteAddr());
        } catch (RuntimeException e) {
            log.debug("Visitor tracking skipped: {}", e.getMessage());
        }
        chain.doFilter(request, response);
    }
}
