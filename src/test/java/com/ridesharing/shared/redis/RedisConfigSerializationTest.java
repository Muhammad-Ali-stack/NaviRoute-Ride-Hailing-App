package com.ridesharing.shared.redis;

import com.ridesharing.pricing.model.FareRule;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Regression: the Redis value serializer used to fail on FareRule's LocalDateTime fields
 * ("Java 8 date/time type not supported by default"). Uses the real serializer from
 * RedisConfig — no mocked Redis operations.
 */
class RedisConfigSerializationTest {

    private final GenericJackson2JsonRedisSerializer serializer = RedisConfig.jsonValueSerializer();

    @Test
    @DisplayName("FareRule with populated date/time fields round-trips through the Redis serializer")
    void fareRuleRoundTrip() {
        LocalDateTime created = LocalDateTime.of(2026, 10, 29, 21, 15, 30, 123_000_000);
        LocalDateTime updated = LocalDateTime.of(2026, 10, 30, 8, 0, 0);
        FareRule rule = FareRule.builder()
                .id(3L).vehicleType("SEDAN")
                .baseFare(new BigDecimal("50.00")).perKmRate(new BigDecimal("15.00"))
                .perMinuteRate(new BigDecimal("2.00")).minimumFare(new BigDecimal("60.00"))
                .active(true).createdAt(created).updatedAt(updated)
                .build();

        byte[] bytes = serializer.serialize(rule);
        Object restored = serializer.deserialize(bytes);

        FareRule back = assertInstanceOf(FareRule.class, restored);
        assertEquals(rule.getId(), back.getId());
        assertEquals("SEDAN", back.getVehicleType());
        assertEquals(0, rule.getBaseFare().compareTo(back.getBaseFare()));
        assertEquals(0, rule.getPerKmRate().compareTo(back.getPerKmRate()));
        assertEquals(0, rule.getPerMinuteRate().compareTo(back.getPerMinuteRate()));
        assertEquals(0, rule.getMinimumFare().compareTo(back.getMinimumFare()));
        assertTrue(back.isActive());
        assertEquals(created, back.getCreatedAt());
        assertEquals(updated, back.getUpdatedAt());
    }

    @Test
    @DisplayName("Dates are written as ISO-8601 strings")
    void datesAreIsoStrings() {
        FareRule rule = FareRule.builder().vehicleType("AUTO")
                .baseFare(BigDecimal.ONE).perKmRate(BigDecimal.ONE)
                .perMinuteRate(BigDecimal.ONE).minimumFare(BigDecimal.ONE)
                .createdAt(LocalDateTime.of(2026, 1, 2, 3, 4, 5)).build();

        String json = new String(serializer.serialize(rule), StandardCharsets.UTF_8);

        assertTrue(json.contains("2026-01-02T03:04:05"), json);
    }

    @Test
    @DisplayName("Surge multipliers (plain doubles) keep their existing format and type")
    void surgeValueRoundTrip() {
        Object back = serializer.deserialize(serializer.serialize(1.5));

        assertEquals(1.5, assertInstanceOf(Number.class, back).doubleValue());
    }
}
