package com.ridesharing.ride.service;

import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.driver.service.DriverLocationService;
import com.ridesharing.pricing.dto.FareEstimateRequest;
import com.ridesharing.pricing.dto.FareEstimateResponse;
import com.ridesharing.pricing.service.PricingService;
import com.ridesharing.ride.dto.RideRequestDto;
import com.ridesharing.ride.dto.RideResponseDto;
import com.ridesharing.ride.events.RideEvent;
import com.ridesharing.ride.model.Ride;
import com.ridesharing.ride.repository.RideRepository;
import com.ridesharing.shared.enums.DriverApprovalStatus;
import com.ridesharing.shared.enums.DriverAvailability;
import com.ridesharing.shared.enums.RideStatus;
import com.ridesharing.shared.enums.UserRole;
import com.ridesharing.shared.exceptions.BadRequestException;
import com.ridesharing.shared.exceptions.ResourceNotFoundException;
import com.ridesharing.shared.kafka.KafkaTopics;
import com.ridesharing.shared.tx.AfterCommit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Orchestrates the full ride lifecycle:
 *   1. Request a ride (rider)
 *   2. Start a ride (driver)
 *   3. Complete a ride (driver)
 *   4. Cancel a ride (rider or driver)
 *   5. Get ride history
 *
 * This service coordinates between:
 *   - PricingService (fare calculation)
 *   - RideMatchingService (async driver matching)
 *   - KafkaTemplate (event publishing)
 */
@Service
public class RideService {

    private static final Logger log = LoggerFactory.getLogger(RideService.class);

    private final RideRepository rideRepository;
    private final UserRepository userRepository;
    private final PricingService pricingService;
    private final RideMatchingService rideMatchingService;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final DriverLocationService driverLocationService;

    public RideService(RideRepository rideRepository,
                       UserRepository userRepository,
                       PricingService pricingService,
                       RideMatchingService rideMatchingService,
                       KafkaTemplate<String, Object> kafkaTemplate,
                       DriverLocationService driverLocationService) {
        this.rideRepository = rideRepository;
        this.userRepository = userRepository;
        this.pricingService = pricingService;
        this.rideMatchingService = rideMatchingService;
        this.kafkaTemplate = kafkaTemplate;
        this.driverLocationService = driverLocationService;
    }

    /**
     * Step 1: Rider requests a ride.
     * - Validates rider isn't already on a ride
     * - Calculates fare estimate via PricingService
     * - Saves ride with status REQUESTED
     * - Publishes ride-requested Kafka event
     * - Triggers async driver matching
     *
     * Intentionally NOT @Transactional: the ride row must be committed before the
     * async matcher starts, otherwise the matcher could not see (and assign) it.
     */
    public RideResponseDto requestRide(Long riderId, RideRequestDto request) {
        User rider = userRepository.findById(riderId)
                .orElseThrow(() -> new ResourceNotFoundException("User", "id", riderId));

        if (rider.getRole() != UserRole.RIDER) {
            throw new BadRequestException("Only riders can request rides");
        }

        // Check if rider already has an active ride
        boolean hasActiveRide = rideRepository.existsByRiderIdAndStatusIn(
                riderId, List.of(RideStatus.REQUESTED, RideStatus.ACCEPTED, RideStatus.IN_PROGRESS));
        if (hasActiveRide) {
            throw new BadRequestException("You already have an active ride");
        }

        // Get fare estimate from Pricing Module
        FareEstimateResponse fareEstimate = pricingService.estimateFare(
                FareEstimateRequest.builder()
                        .pickupLatitude(request.getPickupLatitude())
                        .pickupLongitude(request.getPickupLongitude())
                        .dropoffLatitude(request.getDropoffLatitude())
                        .dropoffLongitude(request.getDropoffLongitude())
                        .vehicleType(request.getVehicleType())
                        .build()
        );

        // Create and save the ride
        Ride ride = Ride.builder()
                .rider(rider)
                .pickupLatitude(request.getPickupLatitude())
                .pickupLongitude(request.getPickupLongitude())
                .pickupAddress(request.getPickupAddress())
                .dropoffLatitude(request.getDropoffLatitude())
                .dropoffLongitude(request.getDropoffLongitude())
                .dropoffAddress(request.getDropoffAddress())
                .vehicleType(request.getVehicleType().toUpperCase())
                .status(RideStatus.REQUESTED)
                .distanceKm(fareEstimate.getDistanceInKm())
                .estimatedTimeMin(fareEstimate.getEstimatedTimeInMinutes())
                .estimatedFare(fareEstimate.getEstimatedFare())
                .surgeMultiplier(fareEstimate.getSurgeMultiplier())
                .build();

        // The check above is only a fast path. Two concurrent requests can both pass it;
        // the uq_rides_rider_active partial unique index (V12) rejects the second insert.
        try {
            ride = rideRepository.save(ride);
        } catch (DataIntegrityViolationException ex) {
            log.warn("Concurrent ride request rejected for rider {}", riderId);
            throw new BadRequestException("You already have an active ride");
        }

        // Publish event + trigger async matching
        publishRideEvent(ride, KafkaTopics.RIDE_REQUESTED);
        rideMatchingService.matchDriverForRide(ride);

        log.info("Ride {} requested by rider {} — fare estimate: {}",
                ride.getId(), riderId, fareEstimate.getEstimatedFare());

        return toResponseDto(ride);
    }

    /**
     * Step 2: Driver starts the ride (picks up the rider).
     * Transition: ACCEPTED → IN_PROGRESS
     *
     * Transactional: the ride row is locked, and the response is mapped inside the
     * transaction (lazy rider/driver are loadable). A mapping failure rolls the transition back.
     */
    @Transactional
    public RideResponseDto startRide(Long driverId, Long rideId) {
        Ride ride = rideRepository.findByIdAndDriverId(rideId, driverId)
                .orElseThrow(() -> new ResourceNotFoundException("Ride", "id", rideId));

        if (ride.getStatus() != RideStatus.ACCEPTED) {
            throw new BadRequestException("Ride can only be started when status is ACCEPTED");
        }

        ride.setStatus(RideStatus.IN_PROGRESS);
        ride.setStartedAt(LocalDateTime.now());
        ride = rideRepository.save(ride);

        log.info("Ride {} started by driver {}", rideId, driverId);
        return toResponseDto(ride);
    }

    /**
     * Step 3: Driver completes the ride (reaches destination).
     * Transition: IN_PROGRESS → COMPLETED
     * Publishes ride-completed event (consumed by Payment + Notification modules)
     * only after the transaction commits.
     */
    @Transactional
    public RideResponseDto completeRide(Long driverId, Long rideId) {
        Ride ride = rideRepository.findByIdAndDriverId(rideId, driverId)
                .orElseThrow(() -> new ResourceNotFoundException("Ride", "id", rideId));

        if (ride.getStatus() != RideStatus.IN_PROGRESS) {
            throw new BadRequestException("Ride can only be completed when status is IN_PROGRESS");
        }

        ride.setStatus(RideStatus.COMPLETED);
        ride.setCompletedAt(LocalDateTime.now());
        ride.setActualFare(ride.getEstimatedFare());
        ride = rideRepository.save(ride);

        // Make driver available again (only if still eligible); they rejoin the matching
        // pool at their last reported position, or at the drop-off point
        releaseDriver(ride.getDriver(), ride.getDropoffLatitude(), ride.getDropoffLongitude());

        publishRideEvent(ride, KafkaTopics.RIDE_COMPLETED);

        log.info("Ride {} completed by driver {} — fare: {}", rideId, driverId, ride.getActualFare());
        return toResponseDto(ride);
    }

    /**
     * Step 4: Cancel a ride (by rider or driver).
     * Allowed transitions: REQUESTED → CANCELLED, ACCEPTED → CANCELLED
     *
     * The ride row is locked so cancellation and async driver assignment serialize:
     * whichever commits first wins, and the other sees the committed state.
     */
    @Transactional
    public RideResponseDto cancelRide(Long userId, Long rideId) {
        Ride ride = rideRepository.findByIdForUpdate(rideId)
                .orElseThrow(() -> new ResourceNotFoundException("Ride", "id", rideId));

        // Verify the user is the rider or the assigned driver
        boolean isRider = ride.getRider().getId().equals(userId);
        boolean isDriver = ride.getDriver() != null && ride.getDriver().getId().equals(userId);
        if (!isRider && !isDriver) {
            throw new BadRequestException("You are not part of this ride");
        }

        if (ride.getStatus() != RideStatus.REQUESTED && ride.getStatus() != RideStatus.ACCEPTED) {
            throw new BadRequestException("Ride can only be cancelled when status is REQUESTED or ACCEPTED");
        }

        ride.setStatus(RideStatus.CANCELLED);
        ride.setCancelledAt(LocalDateTime.now());
        ride = rideRepository.save(ride);

        // If a driver was assigned, make them available again (only if still eligible).
        // Their position is unknown here unless they reported one during the ride.
        if (ride.getDriver() != null) {
            releaseDriver(ride.getDriver(), null, null);
        }

        publishRideEvent(ride, KafkaTopics.RIDE_CANCELLED);

        log.info("Ride {} cancelled by user {}", rideId, userId);
        return toResponseDto(ride);
    }

    /**
     * Get a specific ride by ID (for any authenticated user who is part of the ride).
     */
    @Transactional(readOnly = true)
    public RideResponseDto getRide(Long userId, Long rideId) {
        Ride ride = rideRepository.findById(rideId)
                .orElseThrow(() -> new ResourceNotFoundException("Ride", "id", rideId));

        boolean isRider = ride.getRider().getId().equals(userId);
        boolean isDriver = ride.getDriver() != null && ride.getDriver().getId().equals(userId);
        if (!isRider && !isDriver) {
            throw new BadRequestException("You are not part of this ride");
        }

        return toResponseDto(ride);
    }

    /**
     * Get ride history for a rider.
     */
    @Transactional(readOnly = true)
    public List<RideResponseDto> getRiderHistory(Long riderId) {
        return rideRepository.findByRiderIdOrderByCreatedAtDesc(riderId).stream()
                .map(this::toResponseDto)
                .collect(Collectors.toList());
    }

    /**
     * Get ride history for a driver.
     */
    @Transactional(readOnly = true)
    public List<RideResponseDto> getDriverHistory(Long driverId) {
        return rideRepository.findByDriverIdOrderByCreatedAtDesc(driverId).stream()
                .map(this::toResponseDto)
                .collect(Collectors.toList());
    }

    /**
     * Releases the assigned driver at the end of a ride (completion or cancellation).
     *
     * - Driver no longer BUSY (went OFFLINE, or was rejected/taken offline meanwhile):
     *   availability is left unchanged.
     * - Driver suspended or no longer approved: set OFFLINE, never back into the pool.
     * - Otherwise: set ONLINE and, after commit, put back into the Redis matching pool.
     */
    private void releaseDriver(User driver, Double fallbackLatitude, Double fallbackLongitude) {
        if (driver.getAvailability() != DriverAvailability.BUSY) {
            return;
        }
        if (!driver.isActive() || driver.getApprovalStatus() != DriverApprovalStatus.APPROVED) {
            driver.setAvailability(DriverAvailability.OFFLINE);
            userRepository.save(driver);
            return;
        }

        driver.setAvailability(DriverAvailability.ONLINE);
        userRepository.save(driver);

        Long driverId = driver.getId();
        AfterCommit.run(() -> {
            try {
                driverLocationService.rejoinMatchingPool(driverId, fallbackLatitude, fallbackLongitude);
            } catch (RuntimeException ex) {
                // The ride change is committed; the driver rejoins on their next location update
                log.error("Failed to return driver {} to the matching pool", driverId, ex);
            }
        });
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
                .actualFare(ride.getActualFare())
                .surgeMultiplier(ride.getSurgeMultiplier())
                .timestamp(System.currentTimeMillis())
                .build();

        // Sent only once the ride change is committed; never for a rolled-back transaction
        AfterCommit.run(() -> sendEvent(topic, event));
    }

    private void sendEvent(String topic, RideEvent event) {
        String key = event.getRideId().toString();
        try {
            kafkaTemplate.send(topic, key, event);
            log.info("Published {} event for ride {}", topic, event.getRideId());
        } catch (RuntimeException ex) {
            // The ride change is already committed; report instead of failing the request
            log.error("Failed to publish {} event for ride {}", topic, event.getRideId(), ex);
        }
    }

    private RideResponseDto toResponseDto(Ride ride) {
        return RideResponseDto.builder()
                .rideId(ride.getId())
                .riderId(ride.getRider().getId())
                .riderName(ride.getRider().getName())
                .driverId(ride.getDriver() != null ? ride.getDriver().getId() : null)
                .driverName(ride.getDriver() != null ? ride.getDriver().getName() : null)
                .driverPhone(ride.getDriver() != null ? ride.getDriver().getPhone() : null)
                .pickupLatitude(ride.getPickupLatitude())
                .pickupLongitude(ride.getPickupLongitude())
                .pickupAddress(ride.getPickupAddress())
                .dropoffLatitude(ride.getDropoffLatitude())
                .dropoffLongitude(ride.getDropoffLongitude())
                .dropoffAddress(ride.getDropoffAddress())
                .vehicleType(ride.getVehicleType())
                .status(ride.getStatus())
                .distanceKm(ride.getDistanceKm())
                .estimatedTimeMin(ride.getEstimatedTimeMin())
                .estimatedFare(ride.getEstimatedFare())
                .actualFare(ride.getActualFare())
                .surgeMultiplier(ride.getSurgeMultiplier())
                .requestedAt(ride.getRequestedAt())
                .acceptedAt(ride.getAcceptedAt())
                .startedAt(ride.getStartedAt())
                .completedAt(ride.getCompletedAt())
                .cancelledAt(ride.getCancelledAt())
                .build();
    }
}
