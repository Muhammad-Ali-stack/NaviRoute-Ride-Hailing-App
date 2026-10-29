package com.ridesharing.shared.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionOperations;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Publishes outbox events to Kafka.
 *
 * HOW (every poll, in ONE database transaction):
 *   1. Claim due PENDING events with FOR UPDATE SKIP LOCKED — concurrent publishers (threads or
 *      application instances) never pick the same event.
 *   2. Send each event and WAIT for the broker acknowledgement (acks=all).
 *   3. Acknowledged → PUBLISHED. Failed → stays PENDING with attempts+1 and an exponentially
 *      growing next_attempt_at (capped); the rest of the batch is left for the next poll, so an
 *      unreachable broker does not hold the transaction open for every remaining event.
 *
 * Events live in the database, so anything not yet published is picked up again after a
 * restart. Delivery is AT LEAST ONCE: if the process dies after Kafka acknowledged but before
 * this transaction commits, the event is sent again later. Consumers must be idempotent.
 */
@Component
public class OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /** Payloads are only deserialized into the application's own event classes. */
    private static final String ALLOWED_PAYLOAD_PACKAGE = "com.ridesharing.";
    private static final int MAX_ERROR_LENGTH = 1000;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TransactionOperations transactionOperations;
    private final int batchSize;
    private final long sendTimeoutMs;
    private final long retryBaseDelayMs;
    private final long retryMaxDelayMs;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository,
                           KafkaTemplate<String, Object> kafkaTemplate,
                           ObjectMapper objectMapper,
                           TransactionOperations transactionOperations,
                           @Value("${naviroute.outbox.batch-size:50}") int batchSize,
                           @Value("${naviroute.outbox.send-timeout-ms:10000}") long sendTimeoutMs,
                           @Value("${naviroute.outbox.retry-base-delay-ms:1000}") long retryBaseDelayMs,
                           @Value("${naviroute.outbox.retry-max-delay-ms:300000}") long retryMaxDelayMs) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.transactionOperations = transactionOperations;
        this.batchSize = batchSize;
        this.sendTimeoutMs = sendTimeoutMs;
        this.retryBaseDelayMs = retryBaseDelayMs;
        this.retryMaxDelayMs = retryMaxDelayMs;
    }

    @Scheduled(fixedDelayString = "${naviroute.outbox.poll-interval-ms:1000}",
            initialDelayString = "${naviroute.outbox.initial-delay-ms:1000}")
    public void publishDueEvents() {
        try {
            publishPending(LocalDateTime.now());
        } catch (RuntimeException ex) {
            // e.g. database unavailable — nothing was marked, the next poll retries
            log.error("Outbox publishing run failed", ex);
        }
    }

    /**
     * Publishes the events that are due at {@code now}.
     *
     * @return number of events acknowledged by Kafka and marked PUBLISHED
     */
    public int publishPending(LocalDateTime now) {
        Integer published = transactionOperations.execute(status -> {
            List<OutboxEvent> batch = outboxEventRepository.claimDueEvents(now, batchSize);
            int count = 0;
            for (OutboxEvent event : batch) {
                try {
                    send(event);
                } catch (Exception ex) {
                    if (ex instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    markFailed(event, ex);
                    break;
                }
                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(LocalDateTime.now());
                event.setLastError(null);
                count++;
            }
            return count;
        });
        if (published != null && published > 0) {
            log.info("Outbox: published {} event(s)", published);
        }
        return published == null ? 0 : published;
    }

    private void send(OutboxEvent event) throws Exception {
        Object payload = objectMapper.readValue(event.getPayload(), payloadClass(event.getPayloadType()));
        try {
            // .get(...) waits for the broker acknowledgement; only then is the event PUBLISHED
            kafkaTemplate.send(event.getTopic(), event.getEventKey(), payload)
                    .get(sendTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException ex) {
            throw ex.getCause() instanceof Exception cause ? cause : ex;
        }
    }

    private void markFailed(OutboxEvent event, Exception ex) {
        int attempts = event.getAttempts() + 1;
        long delayMs = backoffMillis(attempts, retryBaseDelayMs, retryMaxDelayMs);
        event.setAttempts(attempts);
        event.setNextAttemptAt(LocalDateTime.now().plusNanos(delayMs * 1_000_000));
        String message = ex.getClass().getSimpleName() + ": " + ex.getMessage();
        event.setLastError(message.length() > MAX_ERROR_LENGTH ? message.substring(0, MAX_ERROR_LENGTH) : message);
        log.warn("Outbox event {} (topic {}, key {}) failed attempt {} — retrying in {} ms: {}",
                event.getId(), event.getTopic(), event.getEventKey(), attempts, delayMs, message);
    }

    /** base × 2^(attempts−1), capped at max. */
    static long backoffMillis(int attempts, long baseDelayMs, long maxDelayMs) {
        int exponent = Math.min(Math.max(attempts - 1, 0), 30);
        long delay = baseDelayMs * (1L << exponent);
        return delay <= 0 || delay > maxDelayMs ? maxDelayMs : delay;
    }

    private static Class<?> payloadClass(String className) throws ClassNotFoundException {
        if (className == null || !className.startsWith(ALLOWED_PAYLOAD_PACKAGE)) {
            throw new IllegalArgumentException("Outbox payload type not allowed: " + className);
        }
        return Class.forName(className);
    }
}
