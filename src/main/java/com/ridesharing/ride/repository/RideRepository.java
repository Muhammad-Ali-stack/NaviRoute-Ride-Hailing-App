package com.ridesharing.ride.repository;

import com.ridesharing.auth.model.User;
import com.ridesharing.ride.model.Ride;
import com.ridesharing.shared.enums.RideStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

@Repository
public interface RideRepository extends JpaRepository<Ride, Long> {

    // rider and driver are fetched with the rides so response mapping needs no extra queries
    @EntityGraph(attributePaths = {"rider", "driver"})
    List<Ride> findByRiderIdOrderByCreatedAtDesc(Long riderId);

    @EntityGraph(attributePaths = {"rider", "driver"})
    List<Ride> findByDriverIdOrderByCreatedAtDesc(Long driverId);

    Optional<Ride> findByIdAndRiderId(Long id, Long riderId);

    /**
     * Row-locks the ride (SELECT ... FOR UPDATE) for driver-side transitions (start/complete),
     * so they cannot interleave with cancellation or the async matcher.
     * Must be called inside a transaction.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Ride> findByIdAndDriverId(Long id, Long driverId);

    /** Row-locks the ride for cancellation. Must be called inside a transaction. */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT r FROM Ride r WHERE r.id = :id")
    Optional<Ride> findByIdForUpdate(@Param("id") Long id);

    List<Ride> findByRiderIdAndStatus(Long riderId, RideStatus status);

    List<Ride> findByDriverIdAndStatus(Long driverId, RideStatus status);

    boolean existsByRiderIdAndStatusIn(Long riderId, java.util.Collection<RideStatus> statuses);

    boolean existsByDriverIdAndStatusIn(Long driverId, java.util.Collection<RideStatus> statuses);

    /**
     * Assigns a driver only if the ride is still REQUESTED.
     * Returns 0 when the ride was cancelled (or already assigned) in the meantime,
     * so a stale matching thread can never turn a CANCELLED ride back into ACCEPTED.
     */
    @Modifying
    @Query("UPDATE Ride r SET r.driver = :driver, "
            + "r.status = com.ridesharing.shared.enums.RideStatus.ACCEPTED, "
            + "r.acceptedAt = :now, r.updatedAt = :now "
            + "WHERE r.id = :rideId AND r.status = com.ridesharing.shared.enums.RideStatus.REQUESTED")
    int assignDriverIfRequested(@Param("rideId") Long rideId,
                                @Param("driver") User driver,
                                @Param("now") LocalDateTime now);

    /** Cancels the ride only if it is still REQUESTED. Returns the number of rows updated. */
    @Modifying
    @Query("UPDATE Ride r SET r.status = com.ridesharing.shared.enums.RideStatus.CANCELLED, "
            + "r.cancelledAt = :now, r.updatedAt = :now "
            + "WHERE r.id = :rideId AND r.status = com.ridesharing.shared.enums.RideStatus.REQUESTED")
    int cancelIfStillRequested(@Param("rideId") Long rideId, @Param("now") LocalDateTime now);
}
