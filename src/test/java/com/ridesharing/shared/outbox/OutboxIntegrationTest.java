package com.ridesharing.shared.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.ride.events.RideEvent;
import com.ridesharing.ride.model.Ride;
import com.ridesharing.ride.repository.RideRepository;
import com.ridesharing.ride.service.RideService;
import com.ridesharing.shared.enums.DriverApprovalStatus;
import com.ridesharing.shared.enums.DriverAvailability;
import com.ridesharing.shared.enums.RideStatus;
import com.ridesharing.shared.enums.UserRole;
import com.ridesharing.shared.kafka.KafkaTopics;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.IllegalTransactionStateException;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Transactional outbox against REAL embedded PostgreSQL and Kafka (the payment consumer runs
 * for real). The application's own scheduled OutboxPublisher is running during these tests;
 * tests that need full control create events that are only due one hour from now and drive
 * dedicated publisher instances with that "now", so the background publisher never sees them.
 * Only the failure test replaces Kafka with a mock that refuses every send.
 */
class OutboxIntegrationTest extends IntegrationTestBase {

    private static final String TEST_TOPIC = "outbox-it";

    @Autowired private RideService rideService;
    @Autowired private OutboxService outboxService;
    @Autowired private OutboxPublisher outboxPublisher;
    @Autowired private OutboxEventRepository outboxEventRepository;
    @Autowired private RideRepository rideRepository;
    @Autowired private UserRepository userRepository;
    @Autowired private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;
    @Autowired private TransactionTemplate transactionTemplate;

    // ── fixtures ──

    private User saveUser(UserRole role) {
        User.UserBuilder b = User.builder().name(role + " outbox").email(UUID.randomUUID() + "@naviroute.test")
                .password("not-used").phone("9000000000").role(role).active(true);
        if (role == UserRole.DRIVER) {
            b.approvalStatus(DriverApprovalStatus.APPROVED).availability(DriverAvailability.BUSY);
        }
        return userRepository.save(b.build());
    }

    private Ride saveRide(RideStatus status) {
        return rideRepository.save(Ride.builder()
                .rider(saveUser(UserRole.RIDER)).driver(saveUser(UserRole.DRIVER))
                .pickupLatitude(PICKUP_LAT).pickupLongitude(PICKUP_LNG)
                .dropoffLatitude(DROPOFF_LAT).dropoffLongitude(DROPOFF_LNG)
                .vehicleType("AUTO").status(status)
                .estimatedFare(new BigDecimal("150.00"))
                .actualFare(status == RideStatus.COMPLETED ? new BigDecimal("150.00") : null)
                .surgeMultiplier(BigDecimal.ONE).build());
    }

    private RideEvent completedEvent(Ride ride) {
        return RideEvent.builder().rideId(ride.getId()).riderId(ride.getRider().getId())
                .driverId(ride.getDriver().getId()).status("COMPLETED")
                .estimatedFare(ride.getEstimatedFare()).actualFare(ride.getActualFare())
                .timestamp(System.currentTimeMillis()).build();
    }

    /** Stores an outbox event that only becomes due in one hour (invisible to the background publisher). */
    private Long enqueueDueInOneHour(String topic, String key, Object payload) {
        return transactionTemplate.execute(tx -> {
            OutboxEvent e = outboxService.enqueue(topic, key, payload);
            e.setNextAttemptAt(LocalDateTime.now().plusHours(1));
            return e.getId();
        });
    }

    private OutboxPublisher publisherWith(KafkaTemplate<String, Object> template, int batchSize, long retryBaseMs) {
        return new OutboxPublisher(outboxEventRepository, template, objectMapper, transactionTemplate,
                batchSize, 10_000, retryBaseMs, 300_000);
    }

    private Map<String, Object> outboxRow(Long id) {
        return jdbc.queryForMap("SELECT status, attempts, last_error, next_attempt_at, published_at "
                + "FROM outbox_events WHERE id = ?", id);
    }

    private int paymentsFor(Long rideId) {
        return jdbc.queryForObject("SELECT COUNT(*) FROM payments WHERE ride_id = ?", Integer.class, rideId);
    }

    // ── tests ──

    @Test
    @DisplayName("Completing a ride stores the ride-completed event in the same transaction; it is published and a PENDING payment created")
    void completionCreatesOutboxEventThatIsPublished() {
        Ride ride = saveRide(RideStatus.IN_PROGRESS);

        rideService.completeRide(ride.getDriver().getId(), ride.getId());

        Map<String, Object> row = jdbc.queryForMap("SELECT id, topic, payload_type, payload FROM outbox_events "
                + "WHERE event_key = ? AND topic = ?", ride.getId().toString(), KafkaTopics.RIDE_COMPLETED);
        assertEquals(RideEvent.class.getName(), row.get("payload_type"));
        assertTrue(row.get("payload").toString().contains("\"status\":\"COMPLETED\""));

        Long id = ((Number) row.get("id")).longValue();
        await().atMost(Duration.ofSeconds(20)).until(() -> "PUBLISHED".equals(outboxRow(id).get("status")));
        assertNotNull(outboxRow(id).get("published_at"));

        await().atMost(Duration.ofSeconds(20)).until(() -> paymentsFor(ride.getId()) == 1);
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM payments WHERE ride_id = ?",
                String.class, ride.getId()), "completing a ride must not mark the payment paid");
    }

    @Test
    @DisplayName("Rolling back the completion removes both the COMPLETED status and the outbox event")
    void rollbackRemovesRideChangeAndOutboxEvent() {
        Ride ride = saveRide(RideStatus.IN_PROGRESS);

        transactionTemplate.executeWithoutResult(tx -> {
            rideService.completeRide(ride.getDriver().getId(), ride.getId());
            assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM outbox_events WHERE event_key = ?",
                    Integer.class, ride.getId().toString()), "visible inside the transaction");
            tx.setRollbackOnly();
        });

        assertEquals("IN_PROGRESS", jdbc.queryForObject("SELECT status FROM rides WHERE id = ?", String.class, ride.getId()));
        assertEquals(0, jdbc.queryForObject("SELECT COUNT(*) FROM outbox_events WHERE event_key = ?",
                Integer.class, ride.getId().toString()));
        assertEquals("BUSY", jdbc.queryForObject("SELECT availability FROM users WHERE id = ?",
                String.class, ride.getDriver().getId()));
    }

    @Test
    @DisplayName("An outbox event cannot be stored outside a transaction")
    void enqueueRequiresTransaction() {
        assertThrows(IllegalTransactionStateException.class,
                () -> outboxService.enqueue(TEST_TOPIC, "k", RideEvent.builder().rideId(1L).build()));
    }

    @Test
    @DisplayName("Kafka failure keeps the event PENDING with backoff; a later retry publishes it and creates the payment")
    void kafkaFailureThenRetry() {
        Ride ride = saveRide(RideStatus.COMPLETED);
        Long id = enqueueDueInOneHour(KafkaTopics.RIDE_COMPLETED, ride.getId().toString(), completedEvent(ride));

        // Kafka refuses every send (mock) — database side is real
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, Object> failingKafka = mock(KafkaTemplate.class);
        when(failingKafka.send(anyString(), anyString(), any()))
                .thenReturn(java.util.concurrent.CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        LocalDateTime dueTime = LocalDateTime.now().plusHours(1).plusMinutes(1);

        assertEquals(0, publisherWith(failingKafka, 50, 60_000).publishPending(dueTime));

        Map<String, Object> failed = outboxRow(id);
        assertEquals("PENDING", failed.get("status"));
        assertEquals(1, ((Number) failed.get("attempts")).intValue());
        assertTrue(failed.get("last_error").toString().contains("broker unavailable"));
        assertTrue(((java.sql.Timestamp) failed.get("next_attempt_at")).toLocalDateTime()
                .isAfter(LocalDateTime.now().plusSeconds(50)), "retry scheduled with backoff");
        assertEquals(0, paymentsFor(ride.getId()));

        // Broker is back: make the event due now; the application's own scheduled publisher retries it
        jdbc.update("UPDATE outbox_events SET next_attempt_at = NOW() WHERE id = ?", id);
        await().atMost(Duration.ofSeconds(20)).until(() -> "PUBLISHED".equals(outboxRow(id).get("status")));
        assertEquals(1, ((Number) outboxRow(id).get("attempts")).intValue());
        await().atMost(Duration.ofSeconds(20)).until(() -> paymentsFor(ride.getId()) == 1);
    }

    @Test
    @DisplayName("Events left PENDING by a previous run are published by a newly started publisher")
    void pendingEventsRecoveredAfterRestart() {
        // Committed before the "crash": rides completed, events never published, one already failed twice
        Ride first = saveRide(RideStatus.COMPLETED);
        Ride second = saveRide(RideStatus.COMPLETED);
        Long firstId = enqueueDueInOneHour(KafkaTopics.RIDE_COMPLETED, first.getId().toString(), completedEvent(first));
        Long secondId = enqueueDueInOneHour(KafkaTopics.RIDE_COMPLETED, second.getId().toString(), completedEvent(second));
        jdbc.update("UPDATE outbox_events SET attempts = 2, last_error = 'previous run' WHERE id = ?", secondId);

        // "Restart": a brand-new publisher with no in-memory state, real Kafka
        OutboxPublisher restarted = publisherWith(kafkaTemplate, 50, 1_000);
        assertTrue(restarted.publishPending(LocalDateTime.now().plusHours(1).plusMinutes(1)) >= 2);

        assertEquals("PUBLISHED", outboxRow(firstId).get("status"));
        assertEquals("PUBLISHED", outboxRow(secondId).get("status"));
        await().atMost(Duration.ofSeconds(20)).until(() -> paymentsFor(first.getId()) == 1 && paymentsFor(second.getId()) == 1);
    }

    @Test
    @DisplayName("The same ride-completed event delivered twice creates exactly one payment")
    void duplicateDeliveryCreatesOnePayment() {
        Ride ride = saveRide(RideStatus.COMPLETED);
        // Two outbox rows for the same ride = re-send after a crash between Kafka ack and commit
        Long a = enqueueDueInOneHour(KafkaTopics.RIDE_COMPLETED, ride.getId().toString(), completedEvent(ride));
        Long b = enqueueDueInOneHour(KafkaTopics.RIDE_COMPLETED, ride.getId().toString(), completedEvent(ride));

        assertTrue(publisherWith(kafkaTemplate, 50, 1_000).publishPending(LocalDateTime.now().plusHours(2)) >= 2);
        assertEquals("PUBLISHED", outboxRow(a).get("status"));
        assertEquals("PUBLISHED", outboxRow(b).get("status"));

        await().atMost(Duration.ofSeconds(20)).until(() -> paymentsFor(ride.getId()) == 1);
        await().pollDelay(Duration.ofSeconds(3)).atMost(Duration.ofSeconds(10)).until(() -> paymentsFor(ride.getId()) == 1);
        assertEquals("PENDING", jdbc.queryForObject("SELECT status FROM payments WHERE ride_id = ?", String.class, ride.getId()));
    }

    @Test
    @DisplayName("Concurrent claims (FOR UPDATE SKIP LOCKED) never return the same event")
    void concurrentClaimsAreDisjoint() throws Exception {
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            ids.add(enqueueDueInOneHour(TEST_TOPIC, "claim-" + i, RideEvent.builder().rideId((long) i).build()));
        }
        LocalDateTime due = LocalDateTime.now().plusHours(1).plusMinutes(1);

        CountDownLatch firstClaimed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(1);
        try {
            Future<List<Long>> holder = pool.submit(() -> transactionTemplate.execute(tx -> {
                List<Long> claimed = outboxEventRepository.claimDueEvents(due, 3).stream().map(OutboxEvent::getId).toList();
                firstClaimed.countDown();
                try {
                    release.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return claimed;
            }));
            assertTrue(firstClaimed.await(20, TimeUnit.SECONDS));

            List<Long> secondClaim = transactionTemplate.execute(tx ->
                    outboxEventRepository.claimDueEvents(due, 10).stream().map(OutboxEvent::getId)
                            .filter(ids::contains).toList());
            release.countDown();
            List<Long> firstClaim = holder.get(20, TimeUnit.SECONDS);

            assertEquals(3, firstClaim.size());
            assertEquals(3, secondClaim.size(), "the locked rows are skipped, not waited for");
            Set<Long> overlap = new HashSet<>(firstClaim);
            overlap.retainAll(secondClaim);
            assertTrue(overlap.isEmpty(), "claims overlap: " + overlap);
        } finally {
            pool.shutdownNow();
            // These rows were only claimed, never sent; take them out of other tests' way
            jdbc.update("UPDATE outbox_events SET status = 'PUBLISHED' WHERE topic = ? AND event_key LIKE 'claim-%'",
                    TEST_TOPIC);
        }
    }

    @Test
    @DisplayName("Two publishers running concurrently send every event exactly once and publish all of them")
    void concurrentPublishersSendEachEventOnce() throws Exception {
        int events = 20;
        List<Long> ids = new ArrayList<>();
        for (int i = 0; i < events; i++) {
            ids.add(enqueueDueInOneHour(TEST_TOPIC, "concurrent-" + i, RideEvent.builder().rideId((long) i).build()));
        }

        // Real Kafka, wrapped to count sends per key across both publishers
        Map<String, AtomicInteger> sends = new ConcurrentHashMap<>();
        KafkaTemplate<String, Object> counting = spy(kafkaTemplate);
        doAnswer(inv -> {
            sends.computeIfAbsent(inv.getArgument(1), k -> new AtomicInteger()).incrementAndGet();
            return inv.callRealMethod();
        }).when(counting).send(anyString(), anyString(), any());

        LocalDateTime due = LocalDateTime.now().plusHours(1).plusMinutes(1);
        OutboxPublisher p1 = publisherWith(counting, 3, 1_000);
        OutboxPublisher p2 = publisherWith(counting, 3, 1_000);
        CyclicBarrier start = new CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            List<Future<Integer>> runs = new ArrayList<>();
            for (OutboxPublisher p : List.of(p1, p2)) {
                runs.add(pool.submit(() -> {
                    start.await();
                    int total = 0;
                    int published;
                    do {
                        published = p.publishPending(due);
                        total += published;
                    } while (published > 0);
                    return total;
                }));
            }
            int total = runs.get(0).get(60, TimeUnit.SECONDS) + runs.get(1).get(60, TimeUnit.SECONDS);
            assertTrue(total >= events, "published " + total);
        } finally {
            pool.shutdownNow();
        }

        for (int i = 0; i < events; i++) {
            assertEquals(1, sends.get("concurrent-" + i).get(), "event concurrent-" + i + " sent more than once");
        }
        for (Long id : ids) {
            assertEquals("PUBLISHED", outboxRow(id).get("status"));
        }
    }
}
