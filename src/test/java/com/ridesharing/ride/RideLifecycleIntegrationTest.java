package com.ridesharing.ride;

import com.fasterxml.jackson.databind.JsonNode;
import com.ridesharing.pricing.model.FareRule;
import com.ridesharing.ride.events.RideEvent;
import com.ridesharing.shared.kafka.KafkaTopics;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

/**
 * End-to-end ride flow over HTTP against real PostgreSQL, Kafka and Redis.
 * Covers the endpoints that previously returned HTTP 500 (lazy loading), the
 * completion → ride-completed → payment path, Redis fare-rule caching and
 * vehicle-type matching.
 */
class RideLifecycleIntegrationTest extends IntegrationTestBase {

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    private Map<String, Object> rideRequest(String vehicleType) {
        return Map.of("pickupLatitude", PICKUP_LAT, "pickupLongitude", PICKUP_LNG,
                "dropoffLatitude", DROPOFF_LAT, "dropoffLongitude", DROPOFF_LNG,
                "pickupAddress", "MG Road", "dropoffAddress", "Koramangala",
                "vehicleType", vehicleType);
    }

    private String rideStatus(long rideId) {
        return jdbc.queryForObject("SELECT status FROM rides WHERE id = ?", String.class, rideId);
    }

    @Test
    @DisplayName("Request → match → details/history → start → complete → payment created → pay → rate")
    void fullRideLifecycle() throws Exception {
        String riderEmail = uniqueEmail("rider");
        String riderToken = register(riderEmail, "RIDER");
        String driverEmail = uniqueEmail("driver");
        String driverToken = onlineDriver(driverEmail, "AUTO", PICKUP_LAT, PICKUP_LNG);
        Long driverId = userId(driverEmail);

        // Request + async matching
        JsonNode requested = body(expectStatus(200, perform(post("/api/rides/request"), riderToken, rideRequest("AUTO"))));
        long rideId = requested.at("/data/rideId").asLong();
        await().atMost(Duration.ofSeconds(15)).until(() -> "ACCEPTED".equals(rideStatus(rideId)));
        assertEquals("BUSY", jdbc.queryForObject("SELECT availability FROM users WHERE id = ?", String.class, driverId));

        // Previously HTTP 500 (LazyInitializationException)
        JsonNode details = body(expectStatus(200, perform(get("/api/rides/" + rideId), riderToken, null)));
        assertEquals(driverId.longValue(), details.at("/data/driverId").asLong());
        assertFalse(details.at("/data/driverName").asText().isBlank());
        assertFalse(details.at("/data/riderName").asText().isBlank());

        JsonNode riderHistory = body(expectStatus(200, perform(get("/api/rides/history/rider"), riderToken, null)));
        assertEquals(rideId, riderHistory.at("/data/0/rideId").asLong());
        assertFalse(riderHistory.at("/data/0/driverName").asText().isBlank());
        expectStatus(200, perform(get("/api/rides/history/driver"), driverToken, null));

        JsonNode started = body(expectStatus(200, perform(put("/api/rides/" + rideId + "/start"), driverToken, null)));
        assertEquals("IN_PROGRESS", started.at("/data/status").asText());

        JsonNode completed = body(expectStatus(200, perform(put("/api/rides/" + rideId + "/complete"), driverToken, null)));
        assertEquals("COMPLETED", completed.at("/data/status").asText());
        assertEquals("COMPLETED", rideStatus(rideId));
        assertEquals("ONLINE", jdbc.queryForObject("SELECT availability FROM users WHERE id = ?", String.class, driverId),
                "driver must be available again after completing the ride");

        // ride-completed (Kafka) → PaymentEventConsumer → PENDING payment
        await().atMost(Duration.ofSeconds(20)).until(() -> paymentCount(rideId) == 1);
        Map<String, Object> payment = jdbc.queryForMap("SELECT id, status, amount FROM payments WHERE ride_id = ?", rideId);
        assertEquals("PENDING", payment.get("status"), "completing a ride must not mark the payment as paid");
        assertEquals(0, new BigDecimal(completed.at("/data/actualFare").asText())
                .compareTo((BigDecimal) payment.get("amount")));

        JsonNode byRide = body(expectStatus(200, perform(get("/api/payments/ride/" + rideId), riderToken, null)));
        assertFalse(byRide.at("/data/driverName").asText().isBlank());
        expectStatus(200, perform(get("/api/payments/history/rider"), riderToken, null));
        expectStatus(200, perform(get("/api/payments/history/driver"), driverToken, null));

        long paymentId = ((Number) payment.get("id")).longValue();
        JsonNode paid = body(expectStatus(200, perform(put("/api/payments/" + paymentId + "/pay"), riderToken,
                Map.of("paymentMethod", "UPI"))));
        assertEquals("COMPLETED", paid.at("/data/status").asText());

        // Ratings — "received" previously HTTP 500
        expectStatus(200, perform(post("/api/ratings"), riderToken, Map.of("rideId", rideId, "score", 5)));
        JsonNode received = body(expectStatus(200, perform(get("/api/ratings/received"), driverToken, null)));
        assertEquals(5, received.at("/data/0/score").asInt());
        assertFalse(received.at("/data/0/raterName").asText().isBlank());
        expectStatus(200, perform(get("/api/ratings/given"), riderToken, null));

        // Duplicate delivery of ride-completed must not create a second payment
        RideEvent duplicate = RideEvent.builder().rideId(rideId).riderId(userId(riderEmail)).driverId(driverId)
                .status("COMPLETED").actualFare(new BigDecimal("1.00")).build();
        kafkaTemplate.send(KafkaTopics.RIDE_COMPLETED, String.valueOf(rideId), duplicate).get();
        kafkaTemplate.send(KafkaTopics.RIDE_COMPLETED, String.valueOf(rideId), duplicate).get();
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10))
                .until(() -> paymentCount(rideId) == 1);
        assertEquals("COMPLETED", jdbc.queryForObject("SELECT status FROM payments WHERE ride_id = ?", String.class, rideId));
    }

    @Test
    @DisplayName("Fare estimate caches the FareRule in real Redis and serves it from the cache")
    void fareEstimateUsesRedisCache() throws Exception {
        String riderToken = register(uniqueEmail("fare-rider"), "RIDER");
        Map<String, Object> estimate = Map.of("pickupLatitude", PICKUP_LAT, "pickupLongitude", PICKUP_LNG,
                "dropoffLatitude", DROPOFF_LAT, "dropoffLongitude", DROPOFF_LNG, "vehicleType", "SEDAN");

        JsonNode first = body(expectStatus(200, perform(post("/api/pricing/estimate"), riderToken, estimate)));

        Object cached = redisTemplate.opsForValue().get("pricing:fare-rule:SEDAN");
        FareRule rule = assertInstanceOf(FareRule.class, cached);
        assertNotNull(rule.getCreatedAt(), "LocalDateTime fields must survive the Redis round trip");
        Long ttl = redisTemplate.getExpire("pricing:fare-rule:SEDAN");
        assertTrue(ttl != null && ttl > 0 && ttl <= 3600, "fare rule cache TTL should be 1 hour, was " + ttl);

        JsonNode second = body(expectStatus(200, perform(post("/api/pricing/estimate"), riderToken, estimate)));
        assertEquals(first.at("/data/estimatedFare").asText(), second.at("/data/estimatedFare").asText());
    }

    @Test
    @DisplayName("SUV request is not matched to an AUTO driver; ride is cancelled and the driver stays ONLINE")
    void vehicleTypeMismatchIsNotMatched() throws Exception {
        String riderToken = register(uniqueEmail("suv-rider"), "RIDER");
        String driverEmail = uniqueEmail("auto-driver");
        onlineDriver(driverEmail, "AUTO", PICKUP_LAT, PICKUP_LNG);
        Long driverId = userId(driverEmail);

        JsonNode requested = body(expectStatus(200, perform(post("/api/rides/request"), riderToken, rideRequest("SUV"))));
        long rideId = requested.at("/data/rideId").asLong();

        // 3 attempts with the 200 ms test retry delay, then "no driver found"
        await().atMost(Duration.ofSeconds(15)).until(() -> "CANCELLED".equals(rideStatus(rideId)));
        assertNull(jdbc.queryForObject("SELECT driver_id FROM rides WHERE id = ?", Long.class, rideId));
        assertEquals("ONLINE", jdbc.queryForObject("SELECT availability FROM users WHERE id = ?", String.class, driverId));
    }

    @Test
    @DisplayName("Rider cancelling an ACCEPTED ride releases the driver")
    void cancelAcceptedRideReleasesDriver() throws Exception {
        String riderToken = register(uniqueEmail("cancel-rider"), "RIDER");
        String driverEmail = uniqueEmail("cancel-driver");
        onlineDriver(driverEmail, "BIKE", PICKUP_LAT, PICKUP_LNG);
        Long driverId = userId(driverEmail);

        long rideId = body(expectStatus(200, perform(post("/api/rides/request"), riderToken, rideRequest("BIKE"))))
                .at("/data/rideId").asLong();
        await().atMost(Duration.ofSeconds(15)).until(() -> "ACCEPTED".equals(rideStatus(rideId)));

        JsonNode cancelled = body(expectStatus(200, perform(put("/api/rides/" + rideId + "/cancel"), riderToken, null)));
        assertEquals("CANCELLED", cancelled.at("/data/status").asText());
        assertEquals("ONLINE", jdbc.queryForObject("SELECT availability FROM users WHERE id = ?", String.class, driverId));
    }

    private int paymentCount(long rideId) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE ride_id = ?", Integer.class, rideId);
        return count == null ? 0 : count;
    }
}
