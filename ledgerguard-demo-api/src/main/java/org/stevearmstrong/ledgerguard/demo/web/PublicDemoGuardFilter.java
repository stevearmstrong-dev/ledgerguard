package org.stevearmstrong.ledgerguard.demo.web;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Semaphore;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

@Component
public class PublicDemoGuardFilter extends OncePerRequestFilter {

    private static final int REQUESTS_PER_WINDOW = 30;
    private static final Duration WINDOW = Duration.ofMinutes(1);

    private final Semaphore concurrentSubmissions = new Semaphore(4);
    private final Deque<Instant> recentSubmissions = new ArrayDeque<>();
    private final Clock clock;

    public PublicDemoGuardFilter() {
        this(Clock.systemUTC());
    }

    PublicDemoGuardFilter(Clock clock) {
        this.clock = clock;
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return true;
        }
        String path = request.getRequestURI();
        return !(path.equals("/api/transactions") || path.startsWith("/api/scenarios/"));
    }

    @Override
    protected void doFilterInternal(
            HttpServletRequest request,
            HttpServletResponse response,
            FilterChain filterChain
    ) throws ServletException, IOException {
        if (!concurrentSubmissions.tryAcquire()) {
            reject(response, "The public demo is processing other transactions. Try again shortly.");
            return;
        }

        try {
            if (!reserveRequest()) {
                reject(response, "The public demo rate limit has been reached. Try again in one minute.");
                return;
            }
            filterChain.doFilter(request, response);
        } finally {
            concurrentSubmissions.release();
        }
    }

    private synchronized boolean reserveRequest() {
        Instant now = clock.instant();
        Instant cutoff = now.minus(WINDOW);
        while (!recentSubmissions.isEmpty() && recentSubmissions.peekFirst().isBefore(cutoff)) {
            recentSubmissions.removeFirst();
        }
        if (recentSubmissions.size() >= REQUESTS_PER_WINDOW) {
            return false;
        }
        recentSubmissions.addLast(now);
        return true;
    }

    private void reject(HttpServletResponse response, String detail) throws IOException {
        response.setStatus(429);
        response.setContentType(MediaType.APPLICATION_PROBLEM_JSON_VALUE);
        response.getWriter().write("{\"title\":\"Too Many Requests\",\"status\":429,\"detail\":\""
                + detail + "\"}");
    }
}
