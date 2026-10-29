package com.ridesharing.shared.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ridesharing.ride.events.RideEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionOperations;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Unit tests (Kafka and repository MOCKED) for the publisher's state transitions. */
@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {

    @Mock
    private OutboxEventRepository repository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private OutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new OutboxPublisher(repository, kafkaTemplate, objectMapper,
                TransactionOperations.withoutTransaction(), 50, 1000, 1000, 300_000);
    }

    private OutboxEvent event(long id, long rideId) throws Exception {
        RideEvent payload = RideEvent.builder().rideId(rideId).riderId(1L).driverId(2L)
                .status("COMPLETED").actualFare(new BigDecimal("99.50")).build();
        return OutboxEvent.builder().id(id).topic("ride-completed").eventKey(String.valueOf(rideId))
                .payloadType(RideEvent.class.getName()).payload(objectMapper.writeValueAsString(payload))
                .nextAttemptAt(LocalDateTime.now()).build();
    }

    @Test
    @DisplayName("Acknowledged send → PUBLISHED, same topic/key and an equivalent RideEvent payload")
    void publishesAndMarksPublished() throws Exception {
        OutboxEvent e = event(1, 100);
        when(repository.claimDueEvents(any(), eq(50))).thenReturn(List.of(e));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenReturn(CompletableFuture.completedFuture(null));

        assertEquals(1, publisher.publishPending(LocalDateTime.now()));

        assertEquals(OutboxStatus.PUBLISHED, e.getStatus());
        assertNotNull(e.getPublishedAt());
        verify(kafkaTemplate).send(eq("ride-completed"), eq("100"), argThat(p ->
                p instanceof RideEvent re && re.getRideId() == 100L
                        && new BigDecimal("99.50").compareTo(re.getActualFare()) == 0));
    }

    @Test
    @DisplayName("Failed send → stays PENDING with attempts, error and backoff; rest of the batch untouched")
    void failureKeepsEventPendingWithBackoff() throws Exception {
        OutboxEvent first = event(1, 100);
        OutboxEvent second = event(2, 101);
        when(repository.claimDueEvents(any(), anyInt())).thenReturn(List.of(first, second));
        when(kafkaTemplate.send(anyString(), anyString(), any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")));

        LocalDateTime before = LocalDateTime.now();
        assertEquals(0, publisher.publishPending(LocalDateTime.now()));

        assertEquals(OutboxStatus.PENDING, first.getStatus());
        assertEquals(1, first.getAttempts());
        assertTrue(first.getLastError().contains("broker down"), first.getLastError());
        assertTrue(first.getNextAttemptAt().isAfter(before.plusNanos(900_000_000L)), "≈1 s backoff");
        assertNull(first.getPublishedAt());

        // The batch stops at the first failure; the second event is not sent or modified
        verify(kafkaTemplate, times(1)).send(anyString(), anyString(), any());
        assertEquals(0, second.getAttempts());
        assertEquals(OutboxStatus.PENDING, second.getStatus());
    }

    @Test
    @DisplayName("Send that throws synchronously is handled like a failed acknowledgement")
    void synchronousSendFailure() throws Exception {
        OutboxEvent e = event(1, 100);
        when(repository.claimDueEvents(any(), anyInt())).thenReturn(List.of(e));
        when(kafkaTemplate.send(anyString(), anyString(), any())).thenThrow(new RuntimeException("metadata timeout"));

        assertEquals(0, publisher.publishPending(LocalDateTime.now()));

        assertEquals(OutboxStatus.PENDING, e.getStatus());
        assertEquals(1, e.getAttempts());
    }

    @Test
    @DisplayName("Payload types outside the application package are never deserialized")
    void rejectsForeignPayloadType() throws Exception {
        OutboxEvent e = event(1, 100);
        e.setPayloadType("java.lang.ProcessBuilder");
        when(repository.claimDueEvents(any(), anyInt())).thenReturn(List.of(e));

        assertEquals(0, publisher.publishPending(LocalDateTime.now()));

        assertEquals(OutboxStatus.PENDING, e.getStatus());
        assertTrue(e.getLastError().contains("not allowed"));
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("Backoff doubles per attempt and is capped")
    void exponentialBackoffIsCapped() {
        assertEquals(1_000, OutboxPublisher.backoffMillis(1, 1_000, 300_000));
        assertEquals(2_000, OutboxPublisher.backoffMillis(2, 1_000, 300_000));
        assertEquals(64_000, OutboxPublisher.backoffMillis(7, 1_000, 300_000));
        assertEquals(300_000, OutboxPublisher.backoffMillis(10, 1_000, 300_000));
        assertEquals(300_000, OutboxPublisher.backoffMillis(500, 1_000, 300_000));
    }
}
