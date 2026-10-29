package com.ridesharing.ride.service;

import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.driver.service.DriverService;
import com.ridesharing.ride.dto.RideRequestDto;
import com.ridesharing.ride.model.Ride;
import com.ridesharing.ride.repository.RideRepository;
import com.ridesharing.ride.service.RideMatchingService.AssignmentResult;
import com.ridesharing.shared.enums.DriverAvailability;
import com.ridesharing.shared.enums.RideStatus;
import com.ridesharing.shared.exceptions.BadRequestException;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.geo.Point;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

/**
 * Concurrency and driver re-entry regression tests against embedded PostgreSQL (row locks,
 * partial unique indexes from V12) and the embedded Redis server (GEO matching pool).
 */
class RideStateHardeningIntegrationTest extends IntegrationTestBase {

    private static final String POOL_KEY = "driver:locations";

    @Autowired private RideService rideService;
    @Autowired private RideMatchingService rideMatchingService;
    @Autowired private DriverService driverService;
    @Autowired private RideRepository rideRepository;
    @Autowired private UserRepository userRepository;
    @Autowired private TransactionTemplate transactionTemplate;

    private User user(String email) {
        return userRepository.findById(userId(email)).orElseThrow();
    }

    private Ride requestedRide(User rider) {
        return rideRepository.save(Ride.builder().rider(rider)
                .pickupLatitude(PICKUP_LAT).pickupLongitude(PICKUP_LNG)
                .dropoffLatitude(DROPOFF_LAT).dropoffLongitude(DROPOFF_LNG)
                .vehicleType("AUTO").status(RideStatus.REQUESTED)
                .estimatedFare(new BigDecimal("100.00")).surgeMultiplier(BigDecimal.ONE).build());
    }

    private String availability(Long userId) {
        return jdbc.queryForObject("SELECT availability FROM users WHERE id = ?", String.class, userId);
    }

    private Point poolPosition(Long driverId) {
        List<Point> positions = redisTemplate.opsForGeo().position(POOL_KEY, driverId.toString());
        return positions == null || positions.isEmpty() ? null : positions.get(0);
    }

    /** Rider + approved ONLINE AUTO driver (via HTTP) + a ride assigned to that driver. */
    private record Assigned(String driverToken, User driver, User rider, Ride ride) {
    }

    private Assigned assignedRide(String label) throws Exception {
        String riderEmail = uniqueEmail(label + "-rider");
        register(riderEmail, "RIDER");
        String driverEmail = uniqueEmail(label + "-driver");
        String driverToken = onlineDriver(driverEmail, "AUTO", PICKUP_LAT, PICKUP_LNG);
        User driver = user(driverEmail);
        Ride ride = requestedRide(user(riderEmail));
        assertEquals(AssignmentResult.ASSIGNED, rideMatchingService.tryAssign(ride, driver));
        assertNull(poolPosition(driver.getId()), "assigned driver leaves the pool");
        return new Assigned(driverToken, driver, user(riderEmail), ride);
    }

    // ── 5a. One active ride per rider ──

    @Test
    @DisplayName("Concurrent ride requests by the same rider: exactly one is created")
    void concurrentRideRequestsSameRider() throws Exception {
        String riderEmail = uniqueEmail("double-request");
        register(riderEmail, "RIDER");
        Long riderId = userId(riderEmail);
        RideRequestDto request = RideRequestDto.builder()
                .pickupLatitude(PICKUP_LAT).pickupLongitude(PICKUP_LNG)
                .dropoffLatitude(DROPOFF_LAT).dropoffLongitude(DROPOFF_LNG).vehicleType("AUTO").build();

        int attempts = 4;
        CyclicBarrier start = new CyclicBarrier(attempts);
        ExecutorService pool = Executors.newFixedThreadPool(attempts);
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < attempts; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return rideService.requestRide(riderId, request);
                }));
            }
            int created = 0;
            int rejected = 0;
            for (Future<?> future : futures) {
                try {
                    future.get(30, TimeUnit.SECONDS);
                    created++;
                } catch (ExecutionException ex) {
                    assertInstanceOf(BadRequestException.class, ex.getCause());
                    rejected++;
                }
            }
            assertEquals(1, created);
            assertEquals(attempts - 1, rejected);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM rides WHERE rider_id = ?", Integer.class, riderId));
    }

    @Test
    @DisplayName("Database rejects a second active ride for the same rider and for the same driver")
    void partialUniqueIndexesEnforced() throws Exception {
        Assigned a = assignedRide("index");

        assertThrows(DataIntegrityViolationException.class, () -> requestedRide(a.rider()));

        // Same driver on a second ACCEPTED ride (bypassing the application) is rejected too
        String otherRider = uniqueEmail("index-other");
        register(otherRider, "RIDER");
        Ride second = requestedRide(user(otherRider));
        assertThrows(DataIntegrityViolationException.class, () -> jdbc.update(
                "UPDATE rides SET driver_id = ?, status = 'ACCEPTED' WHERE id = ?", a.driver().getId(), second.getId()));
    }

    // ── 5b. Driver cannot become available while on an assigned ride ──

    @Test
    @DisplayName("Driver with an ACCEPTED ride cannot switch ONLINE (HTTP 400, stays BUSY)")
    void driverCannotGoOnlineDuringRide() throws Exception {
        Assigned a = assignedRide("online-during-ride");

        expectStatus(400, perform(put("/api/driver/availability?status=ONLINE&latitude=" + PICKUP_LAT
                + "&longitude=" + PICKUP_LNG), a.driverToken(), null));

        assertEquals("BUSY", availability(a.driver().getId()));
        assertNull(poolPosition(a.driver().getId()));
    }

    @Test
    @DisplayName("Going ONLINE waits for an in-flight assignment holding the driver row, then is refused")
    void availabilityChangeSerializesWithAssignment() throws Exception {
        String riderEmail = uniqueEmail("race-rider");
        register(riderEmail, "RIDER");
        String driverEmail = uniqueEmail("race-driver");
        onlineDriver(driverEmail, "AUTO", PICKUP_LAT, PICKUP_LNG);
        User driver = user(driverEmail);
        Ride ride = requestedRide(user(riderEmail));

        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch commit = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            // Thread 1: an assignment that has claimed the driver (row locked) but not committed
            Future<?> assignment = pool.submit(() -> transactionTemplate.executeWithoutResult(tx -> {
                assertEquals(1, userRepository.claimAvailableDriver(driver.getId(), java.time.LocalDateTime.now()));
                assertEquals(1, rideRepository.assignDriverIfRequested(ride.getId(), driver, java.time.LocalDateTime.now()));
                claimed.countDown();
                try {
                    assertTrue(commit.await(20, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));
            assertTrue(claimed.await(20, TimeUnit.SECONDS));

            // Thread 2: the driver tries to (re)confirm ONLINE meanwhile → must wait for the row lock
            Future<?> goOnline = pool.submit(() ->
                    driverService.updateAvailability(driver.getId(), DriverAvailability.ONLINE, PICKUP_LAT, PICKUP_LNG));
            Thread.sleep(500);
            assertFalse(goOnline.isDone(), "availability change must wait for the assignment's row lock");

            commit.countDown();
            assignment.get(20, TimeUnit.SECONDS);

            ExecutionException ex = assertThrows(ExecutionException.class, () -> goOnline.get(20, TimeUnit.SECONDS));
            assertInstanceOf(BadRequestException.class, ex.getCause());
        } finally {
            pool.shutdownNow();
        }
        assertEquals("BUSY", availability(driver.getId()));
        assertEquals("ACCEPTED", jdbc.queryForObject("SELECT status FROM rides WHERE id = ?", String.class, ride.getId()));
    }

    // ── 6. Driver re-entry into the matching pool ──

    @Test
    @DisplayName("After completion the driver is ONLINE and back in the pool at their last reported position")
    void completionReturnsDriverAtLastKnownPosition() throws Exception {
        Assigned a = assignedRide("reentry-ping");
        rideService.startRide(a.driver().getId(), a.ride().getId());

        // While BUSY the driver app keeps reporting its position: remembered, not matchable
        double lat = 12.9400;
        double lng = 77.6200;
        expectStatus(200, perform(put("/api/driver/location"), a.driverToken(), Map.of("latitude", lat, "longitude", lng)));
        assertNull(poolPosition(a.driver().getId()), "a BUSY driver must not be matchable");

        rideService.completeRide(a.driver().getId(), a.ride().getId());

        assertEquals("ONLINE", availability(a.driver().getId()));
        Point position = poolPosition(a.driver().getId());
        assertNotNull(position, "driver must rejoin the matching pool without another location update");
        assertEquals(lat, position.getY(), 0.0005);
        assertEquals(lng, position.getX(), 0.0005);
    }

    @Test
    @DisplayName("Without a reported position the driver rejoins at the drop-off point")
    void completionWithoutPingUsesDropoff() throws Exception {
        Assigned a = assignedRide("reentry-dropoff");
        // Simulate a driver whose position was never recorded (e.g. Redis flushed)
        redisTemplate.delete("driver:last-known-locations");
        rideService.startRide(a.driver().getId(), a.ride().getId());

        rideService.completeRide(a.driver().getId(), a.ride().getId());

        Point position = poolPosition(a.driver().getId());
        assertNotNull(position);
        assertEquals(DROPOFF_LAT, position.getY(), 0.0005);
        assertEquals(DROPOFF_LNG, position.getX(), 0.0005);
    }

    @Test
    @DisplayName("Driver suspended during a ride is set OFFLINE on completion and not re-inserted")
    void suspendedDriverNotReinserted() throws Exception {
        Assigned a = assignedRide("reentry-suspended");
        rideService.startRide(a.driver().getId(), a.ride().getId());
        expectStatus(200, perform(put("/api/admin/users/" + a.driver().getId() + "/suspend"), adminToken(), null));
        assertEquals("BUSY", availability(a.driver().getId()), "the ride keeps its driver");

        rideService.completeRide(a.driver().getId(), a.ride().getId());

        assertEquals("OFFLINE", availability(a.driver().getId()));
        assertNull(poolPosition(a.driver().getId()));
    }

    @Test
    @DisplayName("Driver rejected during a ride stays OFFLINE after completion and is not re-inserted")
    void rejectedDriverNotReinserted() throws Exception {
        Assigned a = assignedRide("reentry-rejected");
        rideService.startRide(a.driver().getId(), a.ride().getId());
        expectStatus(200, perform(put("/api/admin/drivers/" + a.driver().getId() + "/reject"), adminToken(), null));

        rideService.completeRide(a.driver().getId(), a.ride().getId());

        assertEquals("OFFLINE", availability(a.driver().getId()));
        assertNull(poolPosition(a.driver().getId()));
    }

    @Test
    @DisplayName("Cancelling an accepted ride returns the driver to the pool only if a position is known")
    void cancellationReentry() throws Exception {
        Assigned a = assignedRide("reentry-cancel");

        rideService.cancelRide(a.rider().getId(), a.ride().getId());

        assertEquals("ONLINE", availability(a.driver().getId()));
        // The driver went ONLINE at the pickup point (recorded as last known) before assignment
        Point position = poolPosition(a.driver().getId());
        assertNotNull(position);
        assertEquals(PICKUP_LAT, position.getY(), 0.0005);
    }
}
