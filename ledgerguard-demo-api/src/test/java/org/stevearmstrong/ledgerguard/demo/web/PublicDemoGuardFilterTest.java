package org.stevearmstrong.ledgerguard.demo.web;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class PublicDemoGuardFilterTest {

    @Test
    void limitsPublicEventSubmissionsButNotReads() throws Exception {
        PublicDemoGuardFilter filter = new PublicDemoGuardFilter(
                Clock.fixed(Instant.parse("2026-08-16T12:00:00Z"), ZoneOffset.UTC)
        );

        for (int requestNumber = 0; requestNumber < 30; requestNumber++) {
            MockHttpServletResponse accepted = filter("POST", "/api/transactions", filter);
            assertThat(accepted.getStatus()).isEqualTo(200);
        }

        MockHttpServletResponse rejected = filter("POST", "/api/transactions", filter);
        MockHttpServletResponse read = filter("GET", "/api/reconciliations", filter);

        assertThat(rejected.getStatus()).isEqualTo(429);
        assertThat(rejected.getContentAsString()).contains("rate limit");
        assertThat(read.getStatus()).isEqualTo(200);
    }

    private MockHttpServletResponse filter(
            String method,
            String path,
            PublicDemoGuardFilter filter
    ) throws Exception {
        MockHttpServletRequest request = new MockHttpServletRequest(method, path);
        MockHttpServletResponse response = new MockHttpServletResponse();
        filter.doFilter(request, response, new MockFilterChain());
        return response;
    }
}
