package com.ridesharing.payment.consumer;

import com.ridesharing.payment.service.PaymentService;
import com.ridesharing.ride.events.RideEvent;
import com.ridesharing.shared.kafka.KafkaTopics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

/**
 * Listens to ride-completed events and auto-creates payment records.
 * This is the bridge between the Ride Module and Payment Module —
 * they communicate only through Kafka events, not direct method calls.
 */
@Component
public class PaymentEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final PaymentService paymentService;

    public PaymentEventConsumer(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(
            topics = KafkaTopics.RIDE_COMPLETED,
            groupId = "payment-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void onRideCompleted(RideEvent event) {
        log.info("Payment consumer received ride-completed: rideId={}, amount={}",
                event.getRideId(), event.getActualFare());

        BigDecimal amount = event.getActualFare() != null
                ? event.getActualFare()
                : event.getEstimatedFare();

        if (amount == null) {
            log.error("Ride {} has no fare amount — cannot create payment", event.getRideId());
            return;
        }

        try {
            paymentService.createPaymentForRide(
                    event.getRideId(),
                    event.getRiderId(),
                    event.getDriverId(),
                    amount
            );
        } catch (DataIntegrityViolationException ex) {
            // A concurrent delivery of the same event already inserted the payment
            // (uq_payments_ride) — the event is handled, so don't retry it.
            log.warn("Payment for ride {} already exists (concurrent duplicate event) — skipping",
                    event.getRideId());
        }
    }
}
