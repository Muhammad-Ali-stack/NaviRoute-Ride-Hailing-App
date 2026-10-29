package com.ridesharing.shared.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

/**
 * Records a Kafka event in the transactional outbox.
 *
 * WHY: sending to Kafka after commit can lose the event (broker down, process crash between
 *      commit and send). Writing the event into the database in the SAME transaction as the
 *      business change means it is committed — or rolled back — together with that change.
 *      OutboxPublisher then delivers it to Kafka (at least once).
 */
@Service
public class OutboxService {

    private static final Logger log = LoggerFactory.getLogger(OutboxService.class);

    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * Stores the event for publication. MANDATORY: there must be a surrounding transaction —
     * the whole point is that the event commits atomically with the caller's change.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public OutboxEvent enqueue(String topic, String key, Object payload) {
        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            // Fails the caller's transaction: better than committing a change whose event is lost
            throw new IllegalStateException("Cannot serialize outbox payload for topic " + topic, ex);
        }

        OutboxEvent event = outboxEventRepository.save(OutboxEvent.builder()
                .topic(topic)
                .eventKey(key)
                .payloadType(payload.getClass().getName())
                .payload(json)
                .build());
        log.debug("Outbox event {} stored for topic {} key {}", event.getId(), topic, key);
        return event;
    }
}
