package com.ridesharing.shared.outbox;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDateTime;
import java.util.List;

public interface OutboxEventRepository extends JpaRepository<OutboxEvent, Long> {

    /**
     * Claims up to {@code limit} due PENDING events, oldest first, and row-locks them
     * (FOR UPDATE SKIP LOCKED). Rows already locked by another publisher — another thread or
     * another application instance — are skipped, so concurrent publishers never work on the
     * same event. Must be called inside a transaction; the locks are held until it ends.
     */
    @Query(value = "SELECT * FROM outbox_events "
            + "WHERE status = 'PENDING' AND next_attempt_at <= :now "
            + "ORDER BY id LIMIT :limit "
            + "FOR UPDATE SKIP LOCKED", nativeQuery = true)
    List<OutboxEvent> claimDueEvents(@Param("now") LocalDateTime now, @Param("limit") int limit);
}
