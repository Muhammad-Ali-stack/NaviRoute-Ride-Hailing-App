package com.ridesharing.shared.tx;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Defers side effects (Kafka events, Redis updates) until the surrounding
 * database transaction has committed.
 *
 * WHY: publishing "ride-completed" from inside a transaction that later rolls
 * back would tell the Payment module about a ride that was never completed.
 *
 * When no transaction is active the action runs immediately, so callers that
 * are not transactional (and plain unit tests) keep their existing behaviour.
 */
public final class AfterCommit {

    private AfterCommit() {
    }

    public static void run(Runnable action) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    action.run();
                }
            });
        } else {
            action.run();
        }
    }
}
