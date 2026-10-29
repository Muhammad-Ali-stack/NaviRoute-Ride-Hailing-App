package com.ridesharing.ride.service;

import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.driver.repository.VehicleRepository;
import com.ridesharing.driver.service.DriverLocationService;
import com.ridesharing.driver.service.DriverLocationService.DriverLocationResult;
import com.ridesharing.ride.events.RideEvent;
import com.ridesharing.ride.model.Ride;
import com.ridesharing.ride.repository.RideRepository;
import com.ridesharing.shared.enums.DriverApprovalStatus;
import com.ridesharing.shared.enums.DriverAvailability;
import com.ridesharing.shared.enums.RideStatus;
import com.ridesharing.shared.enums.UserRole;
import com.ridesharing.shared.kafka.KafkaTopics;
import com.ridesharing.shared.tx.AfterCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Handles async driver matching when a ride is requested.
 *
 * WHAT: When a rider requests a ride, this service runs IN THE BACKGROUND
 *       to find the nearest available driver and assign them to the ride.
 *
 * WHY:  Driver matching can take seconds (searching, checking availability).
 *       We don't want the rider's HTTP request to hang while we search.
 *       So we return "REQUESTED" immediately and match asynchronously.
 *
 * HOW:  Uses CompletableFuture.supplyAsync() to run on our custom thread pool.
 *       Steps: find nearby drivers → filter available drivers with the requested
 *       vehicle type → atomically claim driver + assign ride.
 *       If no driver found, the ride is cancelled after max retries.
 *
 * CONCURRENCY: the in-memory checks below only pick candidates. The assignment itself
 *       is two conditional UPDATEs in one transaction (driver ONLINE → BUSY, ride
 *       REQUESTED → ACCEPTED), so a driver cannot be given two rides and a ride that
 *       was cancelled meanwhile is never turned back into ACCEPTED.
 */
@Service
public class RideMatchingService {

    private static final Logger log = LoggerFactory.getLogger(RideMatchingService.class);

    private static final double SEARCH_RADIUS_KM = 5.0;
    private static final int MAX_MATCHING_RETRIES = 3;
    private static final long RETRY_DELAY_MS = 5000;
    private static final String PENDING_REQUESTS_KEY = "ride:pending-requests";

    /** Outcome of trying to assign one candidate driver to a ride. */
    enum AssignmentResult {
        ASSIGNED,
        /** Driver was claimed by another ride or is no longer eligible — try the next one. */
        DRIVER_UNAVAILABLE,
        /** Ride is no longer REQUESTED (e.g. the rider cancelled) — stop matching. */
        RIDE_NOT_REQUESTED
    }

    private final DriverLocationService driverLocationService;
    private final UserRepository userRepository;
    private final RideRepository rideRepository;
    private final VehicleRepository vehicleRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, Object> redisTemplate;
    private final Executor asyncExecutor;
    private final TransactionOperations transactionOperations;

    @Value("${naviroute.matching.retry-delay-ms:" + RETRY_DELAY_MS + "}")
    private long retryDelayMs = RETRY_DELAY_MS;

    public RideMatchingService(DriverLocationService driverLocationService,
                               UserRepository userRepository,
                               RideRepository rideRepository,
                               VehicleRepository vehicleRepository,
                               KafkaTemplate<String, Object> kafkaTemplate,
                               RedisTemplate<String, Object> redisTemplate,
                               Executor asyncExecutor,
                               TransactionOperations transactionOperations) {
        this.driverLocationService = driverLocationService;
        this.userRepository = userRepository;
        this.rideRepository = rideRepository;
        this.vehicleRepository = vehicleRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.redisTemplate = redisTemplate;
        this.asyncExecutor = asyncExecutor;
        this.transactionOperations = transactionOperations;
    }

    /**
     * Starts async driver matching for a ride.
     * Called by RideService after saving the ride with status REQUESTED.
     *
     * Uses CompletableFuture to run on the async thread pool.
     * The calling thread (HTTP request thread) is NOT blocked.
     */
    @Async("asyncExecutor")
    public void matchDriverForRide(Ride ride) {
        // Track this as a pending request (used by SurgePricingScheduler)
        redisTemplate.opsForSet().add(PENDING_REQUESTS_KEY, ride.getId().toString());

        log.info("Starting driver matching for ride {} (pickup: {}, {})",
                ride.getId(), ride.getPickupLatitude(), ride.getPickupLongitude());

        CompletableFuture
                .supplyAsync(() -> attemptMatching(ride), asyncExecutor)
                .thenAccept(matched -> {
                    // Remove from pending requests regardless of outcome
                    redisTemplate.opsForSet().remove(PENDING_REQUESTS_KEY, ride.getId().toString());

                    if (!matched) {
                        cancelRideNoDriverFound(ride);
                    }
                })
                .exceptionally(ex -> {
                    log.error("Driver matching failed for ride {}", ride.getId(), ex);
                    redisTemplate.opsForSet().remove(PENDING_REQUESTS_KEY, ride.getId().toString());
                    cancelRideNoDriverFound(ride);
                    return null;
                });
    }

    /**
     * Attempts to find and assign a driver, with retries.
     * Each retry searches for nearby drivers and checks their availability in the DB.
     *
     * @return true if a driver was successfully assigned; false if all retries were
     *         exhausted or the ride stopped being REQUESTED (e.g. cancelled by the rider)
     */
    public boolean attemptMatching(Ride ride) {
        for (int attempt = 1; attempt <= MAX_MATCHING_RETRIES; attempt++) {
            log.info("Matching attempt {}/{} for ride {}", attempt, MAX_MATCHING_RETRIES, ride.getId());

            List<DriverLocationResult> nearbyDrivers = driverLocationService.findNearbyDrivers(
                    ride.getPickupLatitude(),
                    ride.getPickupLongitude(),
                    SEARCH_RADIUS_KM
            );

            if (nearbyDrivers.isEmpty()) {
                log.info("No nearby drivers found for ride {} on attempt {}", ride.getId(), attempt);
                waitBeforeRetry(attempt);
                continue;
            }

            // Try each nearby driver (sorted by distance, nearest first)
            for (DriverLocationResult nearbyDriver : nearbyDrivers) {
                Optional<User> driverOpt = userRepository.findById(nearbyDriver.driverId());
                if (driverOpt.isEmpty()) continue;

                User driver = driverOpt.get();

                // Check: is the driver actually available?
                if (driver.getRole() != UserRole.DRIVER) continue;
                if (driver.getAvailability() != DriverAvailability.ONLINE) continue;
                if (!driver.isActive()) continue;
                if (driver.getApprovalStatus() != DriverApprovalStatus.APPROVED) continue;

                // Check: does the driver's vehicle match the requested vehicle type?
                if (!hasRequestedVehicleType(driver.getId(), ride.getVehicleType())) continue;

                // Check: is this driver already on another active ride?
                boolean onActiveRide = rideRepository.existsByDriverIdAndStatusIn(
                        driver.getId(),
                        List.of(RideStatus.ACCEPTED, RideStatus.IN_PROGRESS)
                );
                if (onActiveRide) continue;

                AssignmentResult result = tryAssign(ride, driver);
                if (result == AssignmentResult.ASSIGNED) {
                    return true;
                }
                if (result == AssignmentResult.RIDE_NOT_REQUESTED) {
                    log.info("Ride {} is no longer REQUESTED — stopping matching", ride.getId());
                    return false;
                }
                // DRIVER_UNAVAILABLE: someone else got this driver first — try the next one
            }

            log.info("All nearby drivers busy for ride {} on attempt {}", ride.getId(), attempt);
            waitBeforeRetry(attempt);
        }

        return false;
    }

    private boolean hasRequestedVehicleType(Long driverId, String requestedVehicleType) {
        return vehicleRepository.findByDriverId(driverId)
                .map(vehicle -> vehicle.getVehicleType().equalsIgnoreCase(requestedVehicleType))
                .orElse(false);
    }

    /**
     * Atomically assigns a driver to a ride in one transaction:
     *   1. claim the driver (ONLINE → BUSY) only if still eligible
     *   2. assign the ride (REQUESTED → ACCEPTED) only if still REQUESTED
     * If step 2 fails the transaction rolls back, releasing the driver claim.
     * The ride-accepted event is published only after commit.
     */
    AssignmentResult tryAssign(Ride ride, User driver) {
        LocalDateTime now = LocalDateTime.now();

        AssignmentResult result;
        try {
            result = transactionOperations.execute(status -> {
                if (userRepository.claimAvailableDriver(driver.getId(), now) == 0) {
                    return AssignmentResult.DRIVER_UNAVAILABLE;
                }
                if (rideRepository.assignDriverIfRequested(ride.getId(), driver, now) == 0) {
                    status.setRollbackOnly();
                    return AssignmentResult.RIDE_NOT_REQUESTED;
                }

                ride.setDriver(driver);
                ride.setStatus(RideStatus.ACCEPTED);
                ride.setAcceptedAt(now);
                driver.setAvailability(DriverAvailability.BUSY);

                publishRideEvent(ride, KafkaTopics.RIDE_ACCEPTED);
                return AssignmentResult.ASSIGNED;
            });
        } catch (DataIntegrityViolationException | ConcurrencyFailureException ex) {
            // uq_rides_driver_active (V12): the driver already has an active ride, or the
            // database aborted a lock conflict. The transaction rolled back — try another driver.
            log.warn("Assignment of driver {} to ride {} rejected by the database: {}",
                    driver.getId(), ride.getId(), ex.getMostSpecificCause().getMessage());
            return AssignmentResult.DRIVER_UNAVAILABLE;
        }

        if (result == AssignmentResult.ASSIGNED) {
            // Remove driver from available pool in Redis (after the DB commit)
            driverLocationService.removeDriverLocation(driver.getId());
            log.info("Ride {} matched with driver {} ({})",
                    ride.getId(), driver.getId(), driver.getName());
        }
        return result;
    }

    /**
     * Cancels a ride when no driver could be found after all retries.
     * Only a ride that is still REQUESTED is cancelled; an already accepted or
     * user-cancelled ride is left untouched and no event is published.
     */
    private void cancelRideNoDriverFound(Ride ride) {
        LocalDateTime now = LocalDateTime.now();

        Boolean cancelled = transactionOperations.execute(status -> {
            if (rideRepository.cancelIfStillRequested(ride.getId(), now) == 0) {
                return false;
            }
            ride.setStatus(RideStatus.CANCELLED);
            ride.setCancelledAt(now);
            publishRideEvent(ride, KafkaTopics.RIDE_CANCELLED);
            return true;
        });

        if (Boolean.TRUE.equals(cancelled)) {
            log.warn("Ride {} cancelled — no driver found after {} attempts",
                    ride.getId(), MAX_MATCHING_RETRIES);
        } else {
            log.info("Ride {} was no longer REQUESTED — not cancelling", ride.getId());
        }
    }

    private void publishRideEvent(Ride ride, String topic) {
        RideEvent event = RideEvent.builder()
                .rideId(ride.getId())
                .riderId(ride.getRider().getId())
                .driverId(ride.getDriver() != null ? ride.getDriver().getId() : null)
                .vehicleType(ride.getVehicleType())
                .status(ride.getStatus().name())
                .pickupLatitude(ride.getPickupLatitude())
                .pickupLongitude(ride.getPickupLongitude())
                .dropoffLatitude(ride.getDropoffLatitude())
                .dropoffLongitude(ride.getDropoffLongitude())
                .estimatedFare(ride.getEstimatedFare())
                .surgeMultiplier(ride.getSurgeMultiplier())
                .timestamp(System.currentTimeMillis())
                .build();

        AfterCommit.run(() -> {
            try {
                kafkaTemplate.send(topic, ride.getId().toString(), event);
                log.info("Published {} event for ride {}", topic, ride.getId());
            } catch (RuntimeException ex) {
                log.error("Failed to publish {} event for ride {}", topic, ride.getId(), ex);
            }
        });
    }

    private void waitBeforeRetry(int attempt) {
        if (attempt < MAX_MATCHING_RETRIES) {
            try {
                Thread.sleep(retryDelayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
