package com.ridesharing.shared.outbox;

// PENDING   = stored with the business change, not yet acknowledged by Kafka (retried until it is)
// PUBLISHED = Kafka acknowledged the write
public enum OutboxStatus {
    PENDING,
    PUBLISHED
}
