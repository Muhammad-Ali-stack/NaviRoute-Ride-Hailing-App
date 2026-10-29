package com.ridesharing.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/**
 * Authorization of the fare-cache endpoint and the Actuator endpoints, exercised through
 * the real Spring Security filter chains (MockMvc, full application context).
 *
 * Network-level restrictions (firewalls, management ports) are NOT covered by these tests.
 */
class EndpointAccessIntegrationTest extends IntegrationTestBase {

    private static MockHttpServletRequestBuilder basic(MockHttpServletRequestBuilder request,
                                                       String username, String password) {
        String credentials = Base64.getEncoder()
                .encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8));
        return request.header("Authorization", "Basic " + credentials);
    }

    // ── 3. Fare-cache invalidation ──

    @Test
    @DisplayName("Fare-cache invalidation: anonymous, rider and driver are forbidden; admin succeeds")
    void fareCacheInvalidationIsAdminOnly() throws Exception {
        String riderToken = register(uniqueEmail("cache-rider"), "RIDER");
        String driverToken = register(uniqueEmail("cache-driver"), "DRIVER");
        redisTemplate.opsForValue().set("pricing:fare-rule:AUTO", "cached");

        expectStatus(403, perform(delete("/api/pricing/cache/AUTO"), null, null));
        expectStatus(403, perform(delete("/api/pricing/cache/AUTO"), riderToken, null));
        expectStatus(403, perform(delete("/api/pricing/cache/AUTO"), driverToken, null));
        assertEquals(Boolean.TRUE, redisTemplate.hasKey("pricing:fare-rule:AUTO"), "cache must be untouched");

        expectStatus(200, perform(delete("/api/pricing/cache/AUTO"), adminToken(), null));
        assertEquals(Boolean.FALSE, redisTemplate.hasKey("pricing:fare-rule:AUTO"));
    }

    @Test
    @DisplayName("Ordinary pricing endpoints remain available to any authenticated user")
    void fareRulesStillAvailable() throws Exception {
        String riderToken = register(uniqueEmail("rules-rider"), "RIDER");

        expectStatus(200, perform(get("/api/pricing/fare-rules"), riderToken, null));
    }

    // ── 4. Actuator ──

    @Test
    @DisplayName("Health and probes are public, without component details for anonymous callers")
    void healthIsPublicWithoutDetails() throws Exception {
        JsonNode health = body(expectStatus(200, perform(get("/actuator/health"), null, null)));
        assertEquals("UP", health.get("status").asText());
        assertNull(health.get("components"), "component details must not be public");

        expectStatus(200, perform(get("/actuator/health/liveness"), null, null));
        expectStatus(200, perform(get("/actuator/health/readiness"), null, null));

        JsonNode adminHealth = body(expectStatus(200, perform(get("/actuator/health"), adminToken(), null)));
        assertNotNull(adminHealth.get("components"), "admins see component details");
    }

    @Test
    @DisplayName("/actuator/metrics: anonymous 401, rider 403, scrape user 403, admin 200")
    void metricsRestrictedToAdmin() throws Exception {
        String riderToken = register(uniqueEmail("metrics-rider"), "RIDER");

        expectStatus(401, perform(get("/actuator/metrics"), null, null));
        expectStatus(403, perform(get("/actuator/metrics"), riderToken, null));
        expectStatus(403, mvc.perform(basic(get("/actuator/metrics"), METRICS_USERNAME, METRICS_PASSWORD)).andReturn());
        expectStatus(200, perform(get("/actuator/metrics"), adminToken(), null));
        expectStatus(403, perform(get("/actuator/info"), riderToken, null));
        expectStatus(403, perform(get("/actuator"), riderToken, null));
    }

    @Test
    @DisplayName("/actuator/prometheus: scrape user and admin allowed; anonymous, wrong password and rider denied")
    void prometheusScrapeAuthentication() throws Exception {
        String riderToken = register(uniqueEmail("prom-rider"), "RIDER");

        MvcResult scraped = expectStatus(200,
                mvc.perform(basic(get("/actuator/prometheus"), METRICS_USERNAME, METRICS_PASSWORD)).andReturn());
        assertTrue(scraped.getResponse().getContentAsString().contains("jvm_memory_used_bytes"));

        expectStatus(401, perform(get("/actuator/prometheus"), null, null));
        expectStatus(401, mvc.perform(basic(get("/actuator/prometheus"), METRICS_USERNAME, "wrong-password")).andReturn());
        expectStatus(403, perform(get("/actuator/prometheus"), riderToken, null));
        expectStatus(200, perform(get("/actuator/prometheus"), adminToken(), null));
    }

    @Test
    @DisplayName("The scrape user cannot authenticate against the API")
    void scrapeUserHasNoApiAccess() throws Exception {
        expectStatus(403, mvc.perform(basic(get("/api/rides/history/rider"), METRICS_USERNAME, METRICS_PASSWORD))
                .andReturn());
        expectStatus(403, mvc.perform(basic(get("/api/admin/drivers"), METRICS_USERNAME, METRICS_PASSWORD))
                .andReturn());
    }

    @Test
    @DisplayName("Endpoints that are not exposed (env, beans) are not reachable, even for admins")
    void unexposedEndpointsAreUnavailable() throws Exception {
        String admin = adminToken();

        // Not exposed → no such endpoint: 404 (previously the catch-all handler returned 500)
        expectStatus(404, perform(get("/actuator/env"), admin, null));
        expectStatus(404, perform(get("/actuator/beans"), admin, null));
        expectStatus(404, perform(get("/api/does-not-exist"), admin, null));
        // Anonymous callers are refused before path resolution
        expectStatus(403, perform(get("/actuator/env"), null, null));
    }
}
