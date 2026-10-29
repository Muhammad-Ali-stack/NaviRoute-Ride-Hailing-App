package com.ridesharing.admin;

import com.fasterxml.jackson.databind.JsonNode;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

/**
 * Regression tests (real PostgreSQL + Kafka + Redis) for:
 *  - admin login (configurable bootstrap instead of the broken V7 seed)
 *  - driver approval state on registration, pending list, rejection and re-approval
 *  - migrations V10/V11 on a fresh database
 */
class AdminAndDriverApprovalIntegrationTest extends IntegrationTestBase {

    @Test
    @DisplayName("Flyway applies V1-V14 on a fresh database and the unusable V7 seed admin is deactivated")
    void migrationsApplied() {
        List<String> versions = jdbc.queryForList(
                "SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank", String.class);
        assertEquals(List.of("1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"), versions);
        assertEquals(1, jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'outbox_events'", Integer.class));

        List<String> indexes = jdbc.queryForList(
                "SELECT indexname FROM pg_indexes WHERE indexname IN "
                        + "('uq_rides_rider_active', 'uq_rides_driver_active', 'uq_users_email_lower') ORDER BY indexname",
                String.class);
        assertEquals(List.of("uq_rides_driver_active", "uq_rides_rider_active", "uq_users_email_lower"), indexes);

        Boolean seedActive = jdbc.queryForObject(
                "SELECT active FROM users WHERE email = 'admin@ridesharing.com'", Boolean.class);
        assertFalse(seedActive);
    }

    @Test
    @DisplayName("Bootstrapped administrator can log in; the V7 seed account cannot")
    void adminLogin() throws Exception {
        assertFalse(adminToken().isBlank());

        expectStatus(401, perform(post("/api/auth/login"), null,
                Map.of("email", "admin@ridesharing.com", "password", "admin123")));
    }

    @Test
    @DisplayName("New driver is PENDING, listed as pending, cannot go online or share location before approval")
    void newDriverIsPending() throws Exception {
        String email = uniqueEmail("pending-driver");
        String driverToken = register(email, "DRIVER");
        Long driverId = userId(email);

        Map<String, Object> row = jdbc.queryForMap(
                "SELECT approval_status, availability FROM users WHERE id = ?", driverId);
        assertEquals("PENDING", row.get("approval_status"));
        assertEquals("OFFLINE", row.get("availability"));

        JsonNode pending = body(expectStatus(200, perform(get("/api/admin/drivers/pending"), adminToken(), null)));
        assertTrue(containsDriver(pending.get("data"), driverId), "pending list should contain the new driver");

        expectStatus(400, perform(put("/api/driver/availability?status=ONLINE&latitude=12.97&longitude=77.59"),
                driverToken, null));
        expectStatus(400, perform(put("/api/driver/location"), driverToken,
                Map.of("latitude", 12.97, "longitude", 77.59)));
        assertNull(redisTemplate.opsForGeo().position("driver:locations", driverId.toString()).get(0));
    }

    @Test
    @DisplayName("Rejected driver is deactivated and offline; re-approval restores login and going online")
    void rejectThenReapprove() throws Exception {
        String email = uniqueEmail("reapproved-driver");
        String driverToken = onlineDriver(email, "AUTO", 12.90, 77.50);
        Long driverId = userId(email);
        String admin = adminToken();

        expectStatus(200, perform(put("/api/admin/drivers/" + driverId + "/reject"), admin, null));

        Map<String, Object> rejected = jdbc.queryForMap(
                "SELECT approval_status, availability, active FROM users WHERE id = ?", driverId);
        assertEquals("REJECTED", rejected.get("approval_status"));
        assertEquals("OFFLINE", rejected.get("availability"));
        assertEquals(false, rejected.get("active"));
        assertNull(redisTemplate.opsForGeo().position("driver:locations", driverId.toString()).get(0),
                "rejected driver must be removed from the matching pool");
        expectStatus(401, perform(post("/api/auth/login"), null, Map.of("email", email, "password", "pass1234")));
        // The token issued before rejection no longer authenticates (account inactive) → 403
        // before reaching the service (previously it reached the service and got 400)
        expectStatus(403, perform(put("/api/driver/location"), driverToken,
                Map.of("latitude", 12.97, "longitude", 77.59)));

        expectStatus(200, perform(put("/api/admin/drivers/" + driverId + "/approve"), admin, null));

        String newToken = login(email, "pass1234");
        expectStatus(200, perform(put("/api/driver/availability?status=ONLINE&latitude=12.90&longitude=77.50"),
                newToken, null));
        assertEquals(true, jdbc.queryForObject("SELECT active FROM users WHERE id = ?", Boolean.class, driverId));
    }

    private static boolean containsDriver(JsonNode drivers, Long driverId) {
        for (JsonNode d : drivers) {
            if (d.get("driverId").asLong() == driverId) {
                return true;
            }
        }
        return false;
    }
}
