package com.ridesharing.auth.repository;

import com.ridesharing.auth.model.User;
import com.ridesharing.shared.enums.DriverApprovalStatus;
import com.ridesharing.shared.enums.UserRole;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Database access layer for the User entity.
 *
 * By extending JpaRepository<User, Long>, we get these methods FOR FREE:
 *   save(user)       → INSERT or UPDATE a user
 *   findById(id)     → SELECT * FROM users WHERE id = ?
 *   findAll()        → SELECT * FROM users
 *   deleteById(id)   → DELETE FROM users WHERE id = ?
 *   count()          → SELECT COUNT(*) FROM users
 *   existsById(id)   → SELECT EXISTS(SELECT 1 FROM users WHERE id = ?)
 *
 * We add custom methods below. Spring reads the method name
 * and auto-generates the SQL. No implementation needed.
 *
 * @Repository marks this as a Spring-managed data access component.
 */
@Repository
public interface UserRepository extends JpaRepository<User, Long> {

    /**
     * Finds a user by email address.
     * Used during LOGIN to verify credentials.
     *
     * Spring auto-generates: SELECT * FROM users WHERE email = ?
     *
     * Returns Optional<User> because the user might not exist.
     * Optional forces the caller to handle the "not found" case explicitly
     * instead of risking a NullPointerException.
     *
     * @param email the email to search for
     * @return Optional containing the user if found, empty if not
     *
     * Email identity is case-insensitive: the lookup compares LOWER(email), which is also
     * what the uq_users_email_lower unique index (V13) enforces.
     */
    @Query("SELECT u FROM User u WHERE LOWER(u.email) = LOWER(:email)")
    Optional<User> findByEmail(@Param("email") String email);

    /**
     * Checks if an email is already registered.
     * Used during REGISTRATION to prevent duplicate accounts.
     *
     * Spring auto-generates: SELECT EXISTS(SELECT 1 FROM users WHERE email = ?)
     *
     * @param email the email to check
     * @return true if email exists (ignoring letter case), false otherwise
     */
    @Query("SELECT COUNT(u) > 0 FROM User u WHERE LOWER(u.email) = LOWER(:email)")
    boolean existsByEmail(@Param("email") String email);

    /**
     * Returns only the account's active flag (empty if the user no longer exists).
     * Used on every authenticated request so that suspended or deactivated accounts
     * cannot keep using previously issued tokens.
     */
    @Query("SELECT u.active FROM User u WHERE u.id = :id")
    Optional<Boolean> findActiveById(@Param("id") Long id);

    /**
     * Loads a user with a row lock (SELECT ... FOR UPDATE). Must be called in a transaction.
     * Used when a driver changes availability, so the change serializes with ride
     * assignment (which claims the same row).
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT u FROM User u WHERE u.id = :id")
    Optional<User> findByIdForUpdate(@Param("id") Long id);

    /**
     * Finds all drivers with a specific approval status.
     * Used by Admin Module to list pending/approved/rejected drivers.
     *
     * Spring auto-generates: SELECT * FROM users WHERE role = ? AND approval_status = ?
     */
    List<User> findByRoleAndApprovalStatus(UserRole role, DriverApprovalStatus approvalStatus);

    /**
     * Finds all users with a given role.
     * Used by Admin Module to list all drivers.
     *
     * Spring auto-generates: SELECT * FROM users WHERE role = ?
     */
    List<User> findByRole(UserRole role);

    /**
     * Atomically claims a driver for a ride: flips ONLINE → BUSY only if the driver is
     * still an active, approved, ONLINE driver. Returns 1 if claimed, 0 otherwise.
     *
     * The single conditional UPDATE is the concurrency guard: two matching threads
     * racing for the same driver serialize on the row lock, and the loser sees BUSY.
     */
    @Modifying
    @Query("UPDATE User u SET u.availability = com.ridesharing.shared.enums.DriverAvailability.BUSY, "
            + "u.updatedAt = :now "
            + "WHERE u.id = :driverId "
            + "AND u.role = com.ridesharing.shared.enums.UserRole.DRIVER "
            + "AND u.active = true "
            + "AND u.approvalStatus = com.ridesharing.shared.enums.DriverApprovalStatus.APPROVED "
            + "AND u.availability = com.ridesharing.shared.enums.DriverAvailability.ONLINE")
    int claimAvailableDriver(@Param("driverId") Long driverId, @Param("now") LocalDateTime now);
}
