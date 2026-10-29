package com.ridesharing.ride.service;

import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.driver.model.Vehicle;
import com.ridesharing.driver.repository.VehicleRepository;
import com.ridesharing.ride.model.Ride;
import com.ridesharing.ride.repository.RideRepository;
import com.ridesharing.ride.service.RideMatchingService.AssignmentResult;
import com.ridesharing.shared.enums.DriverApprovalStatus;
import com.ridesharing.shared.enums.DriverAvailability;
import com.ridesharing.shared.enums.RideStatus;
import com.ridesharing.shared.enums.UserRole;
import com.ridesharing.shared.tx.AfterCommit;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Concurrency and transaction-boundary regression tests against real PostgreSQL row locks.
 */
class MatchingConcurrencyIntegrationTest extends IntegrationTestBase {

    @Autowired private RideMatchingService rideMatchingService;
    @Autowired private RideService rideService;
    @Autowired private RideRepository rideRepository;
    @Autowired private UserRepository userRepository;
    @Autowired private VehicleRepository vehicleRepository;
    @Autowired private TransactionTemplate transactionTemplate;

    private User saveUser(UserRole role) {
        User.UserBuilder builder = User.builder()
                .name(role + " user").email(UUID.randomUUID() + "@naviroute.test")
                .password("not-used").phone("9000000000").role(role).active(true);
        if (role == UserRole.DRIVER) {
            builder.approvalStatus(DriverApprovalStatus.APPROVED).availability(DriverAvailability.ONLINE);
        }
        User user = userRepository.save(builder.build());
        if (role == UserRole.DRIVER) {
            vehicleRepository.save(Vehicle.builder().driver(user).vehicleType("AUTO")
                    .plateNumber("C-" + UUID.randomUUID().toString().substring(0, 12))
                    .model("Model").color("Blue").build());
        }
        return user;
    }

    private Ride saveRequestedRide(User rider) {
        return rideRepository.save(Ride.builder().rider(rider)
                .pickupLatitude(PICKUP_LAT).pickupLongitude(PICKUP_LNG)
                .dropoffLatitude(DROPOFF_LAT).dropoffLongitude(DROPOFF_LNG)
                .vehicleType("AUTO").status(RideStatus.REQUESTED)
                .estimatedFare(new BigDecimal("100.00")).surgeMultiplier(BigDecimal.ONE).build());
    }

    private String status(Ride ride) {
        return jdbc.queryForObject("SELECT status FROM rides WHERE id = ?", String.class, ride.getId());
    }

    private String availability(User user) {
        return jdbc.queryForObject("SELECT availability FROM users WHERE id = ?", String.class, user.getId());
    }

    @Test
    @DisplayName("Two rides racing for one driver: exactly one assignment wins")
    void concurrentAssignmentOfSameDriver() throws Exception {
        User driver = saveUser(UserRole.DRIVER);
        Ride rideA = saveRequestedRide(saveUser(UserRole.RIDER));
        Ride rideB = saveRequestedRide(saveUser(UserRole.RIDER));

        CyclicBarrier start = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<AssignmentResult> a = pool.submit(() -> {
                start.await();
                return rideMatchingService.tryAssign(rideA, driver);
            });
            Future<AssignmentResult> b = pool.submit(() -> {
                start.await();
                return rideMatchingService.tryAssign(rideB, driver);
            });
            List<AssignmentResult> results = List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS));

            assertEquals(1, results.stream().filter(r -> r == AssignmentResult.ASSIGNED).count(), results.toString());
            assertEquals(1, results.stream().filter(r -> r == AssignmentResult.DRIVER_UNAVAILABLE).count(), results.toString());
        } finally {
            pool.shutdownNow();
        }

        Integer acceptedWithDriver = jdbc.queryForObject(
                "SELECT COUNT(*) FROM rides WHERE driver_id = ? AND status = 'ACCEPTED'", Integer.class, driver.getId());
        assertEquals(1, acceptedWithDriver);
        assertEquals("BUSY", availability(driver));
    }

    @Test
    @DisplayName("Stale matcher cannot turn a cancelled ride back into ACCEPTED; the driver claim is rolled back")
    void cancelledRideIsNotResurrected() {
        User rider = saveUser(UserRole.RIDER);
        User driver = saveUser(UserRole.DRIVER);
        Ride ride = saveRequestedRide(rider);

        rideService.cancelRide(rider.getId(), ride.getId());

        AssignmentResult result = rideMatchingService.tryAssign(ride, driver);

        assertEquals(AssignmentResult.RIDE_NOT_REQUESTED, result);
        assertEquals("CANCELLED", status(ride));
        assertNull(jdbc.queryForObject("SELECT driver_id FROM rides WHERE id = ?", Long.class, ride.getId()));
        assertEquals("ONLINE", availability(driver), "driver claim must be rolled back");
    }

    @Test
    @DisplayName("Assignment waits for an in-flight cancellation holding the ride lock, then backs off")
    void assignmentBlockedByCancellationLock() throws Exception {
        User rider = saveUser(UserRole.RIDER);
        User driver = saveUser(UserRole.DRIVER);
        Ride ride = saveRequestedRide(rider);

        CountDownLatch locked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            // Thread 1: a cancellation in progress — holds the ride row lock, then cancels
            Future<?> canceller = pool.submit(() -> transactionTemplate.executeWithoutResult(tx -> {
                Ride locked0 = rideRepository.findByIdForUpdate(ride.getId()).orElseThrow();
                locked.countDown();
                try {
                    assertTrue(release.await(20, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                locked0.setStatus(RideStatus.CANCELLED);
            }));
            assertTrue(locked.await(20, TimeUnit.SECONDS));

            // Thread 2: matcher tries to assign while the lock is held → must block
            Future<AssignmentResult> matcher = pool.submit(() -> rideMatchingService.tryAssign(ride, driver));
            Thread.sleep(500);
            assertFalse(matcher.isDone(), "assignment should wait for the ride row lock");

            release.countDown();
            canceller.get(20, TimeUnit.SECONDS);

            assertEquals(AssignmentResult.RIDE_NOT_REQUESTED, matcher.get(20, TimeUnit.SECONDS));
        } finally {
            pool.shutdownNow();
        }
        assertEquals("CANCELLED", status(ride));
        assertEquals("ONLINE", availability(driver));
    }

    @Test
    @DisplayName("Ineligible (pending) driver cannot be claimed even if selected")
    void pendingDriverCannotBeClaimed() {
        User driver = saveUser(UserRole.DRIVER);
        jdbc.update("UPDATE users SET approval_status = 'PENDING' WHERE id = ?", driver.getId());
        Ride ride = saveRequestedRide(saveUser(UserRole.RIDER));

        assertEquals(AssignmentResult.DRIVER_UNAVAILABLE, rideMatchingService.tryAssign(ride, driver));
        assertEquals("REQUESTED", status(ride));
    }

    @Test
    @DisplayName("Side effects registered with AfterCommit run on commit and never on rollback")
    void afterCommitOnlyOnCommit() {
        AtomicBoolean rolledBackRan = new AtomicBoolean(false);
        transactionTemplate.executeWithoutResult(tx -> {
            AfterCommit.run(() -> rolledBackRan.set(true));
            tx.setRollbackOnly();
        });
        assertFalse(rolledBackRan.get());

        AtomicBoolean committedRan = new AtomicBoolean(false);
        transactionTemplate.executeWithoutResult(tx -> {
            AfterCommit.run(() -> committedRan.set(true));
            assertFalse(committedRan.get(), "must not run before commit");
        });
        assertTrue(committedRan.get());
    }

    @Test
    @DisplayName("Completing a ride whose transition fails publishes nothing and changes nothing")
    void failedCompletionLeavesNoPartialState() {
        User rider = saveUser(UserRole.RIDER);
        User driver = saveUser(UserRole.DRIVER);
        Ride ride = saveRequestedRide(rider);
        assertEquals(AssignmentResult.ASSIGNED, rideMatchingService.tryAssign(ride, driver));

        // Not IN_PROGRESS yet → completion must fail without touching ride or driver
        assertThrows(RuntimeException.class, () -> rideService.completeRide(driver.getId(), ride.getId()));

        assertEquals("ACCEPTED", status(ride));
        assertEquals("BUSY", availability(driver));
        Integer payments = jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE ride_id = ?", Integer.class, ride.getId());
        assertEquals(0, payments);
    }
}
