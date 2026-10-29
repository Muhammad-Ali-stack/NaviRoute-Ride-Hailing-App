package com.ridesharing.auth.service;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.nio.charset.StandardCharsets;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for JwtService — token generation and validation.
 *
 * No Spring context needed — JwtService is a plain class with no DB dependency.
 * We use ReflectionTestUtils to set the private @Value fields directly.
 *
 * Happy path: generate token → extract claims → all correct
 * Failure 1: expired token → isTokenValid returns false
 * Failure 2: tampered token → isTokenValid returns false
 * Failure 3: random garbage string → isTokenValid returns false
 * Edge case: extract userId and role from a valid token
 */
@DisplayName("JwtService Tests")
class JwtServiceTest {

    private JwtService jwtService;

    @BeforeEach
    void setUp() {
        jwtService = new JwtService();

        // Inject @Value fields without starting Spring context
        ReflectionTestUtils.setField(jwtService, "secretKey",
                "myTestSecretKeyThatIsAtLeast32CharactersLongForHS256");
        ReflectionTestUtils.setField(jwtService, "accessTokenExpiry", 900000L);
        ReflectionTestUtils.setField(jwtService, "refreshTokenExpiry", 604800000L);
    }

    @Test
    @DisplayName("should_generateValidAccessToken_when_calledWithUserIdAndRole")
    void should_generateValidAccessToken_when_calledWithUserIdAndRole() {
        // WHEN
        String token = jwtService.generateAccessToken(42L, "RIDER");

        // THEN
        assertNotNull(token);
        assertFalse(token.isEmpty());
        assertTrue(token.contains(".")); // JWT has 3 parts separated by dots
    }

    @Test
    @DisplayName("should_extractCorrectUserId_when_tokenIsValid")
    void should_extractCorrectUserId_when_tokenIsValid() {
        // GIVEN
        String token = jwtService.generateAccessToken(42L, "RIDER");

        // WHEN
        Long userId = jwtService.extractUserId(token);

        // THEN
        assertEquals(42L, userId);
    }

    @Test
    @DisplayName("should_extractCorrectRole_when_tokenIsValid")
    void should_extractCorrectRole_when_tokenIsValid() {
        // GIVEN
        String token = jwtService.generateAccessToken(42L, "DRIVER");

        // WHEN
        String role = jwtService.extractRole(token);

        // THEN
        assertEquals("DRIVER", role);
    }

    @Test
    @DisplayName("should_returnTrue_when_tokenIsValid")
    void should_returnTrue_when_tokenIsValid() {
        // GIVEN
        String token = jwtService.generateAccessToken(1L, "RIDER");

        // WHEN / THEN
        assertTrue(jwtService.isTokenValid(token));
    }

    @Test
    @DisplayName("should_returnFalse_when_tokenIsTampered")
    void should_returnFalse_when_tokenIsTampered() {
        // GIVEN: a valid token with one character changed (tampered)
        String token = jwtService.generateAccessToken(1L, "RIDER");
        String tamperedToken = token.substring(0, token.length() - 1) + "X";

        // WHEN / THEN
        assertFalse(jwtService.isTokenValid(tamperedToken));
    }

    @Test
    @DisplayName("should_returnFalse_when_tokenIsRandomGarbage")
    void should_returnFalse_when_tokenIsRandomGarbage() {
        // WHEN / THEN
        assertFalse(jwtService.isTokenValid("this.is.not.a.jwt"));
    }

    @Test
    @DisplayName("should_returnFalse_when_tokenIsExpired")
    void should_returnFalse_when_tokenIsExpired() {
        // GIVEN: set expiry to 0ms (token expires immediately)
        ReflectionTestUtils.setField(jwtService, "accessTokenExpiry", 0L);
        String token = jwtService.generateAccessToken(1L, "RIDER");

        // WHEN / THEN: token is already expired
        assertFalse(jwtService.isTokenValid(token));
    }

    @Test
    @DisplayName("should_generateDifferentTokens_when_accessVsRefresh")
    void should_generateDifferentTokens_when_accessVsRefresh() {
        // GIVEN
        String accessToken = jwtService.generateAccessToken(42L, "RIDER");
        String refreshToken = jwtService.generateRefreshToken(42L, "RIDER");

        // THEN: they should be different (different expiry times)
        assertFalse(accessToken.equals(refreshToken));
    }

    // ── TOKEN TYPE ENFORCEMENT ──

    private static final String SECRET = "myTestSecretKeyThatIsAtLeast32CharactersLongForHS256";

    @Test
    @DisplayName("access token is accepted as access and rejected as refresh")
    void accessTokenTypeIsEnforced() {
        String access = jwtService.generateAccessToken(7L, "RIDER");

        assertTrue(jwtService.isAccessToken(access));
        assertFalse(jwtService.isRefreshToken(access));
        assertEquals("7", jwtService.parseAccessToken(access).orElseThrow().getSubject());
    }

    @Test
    @DisplayName("refresh token is accepted as refresh and rejected as access")
    void refreshTokenTypeIsEnforced() {
        String refresh = jwtService.generateRefreshToken(7L, "RIDER");

        assertTrue(jwtService.isRefreshToken(refresh));
        assertFalse(jwtService.isAccessToken(refresh));
        assertTrue(jwtService.parseAccessToken(refresh).isEmpty());
    }

    @Test
    @DisplayName("expired access and refresh tokens are rejected")
    void expiredTokensRejected() {
        ReflectionTestUtils.setField(jwtService, "accessTokenExpiry", -1000L);
        ReflectionTestUtils.setField(jwtService, "refreshTokenExpiry", -1000L);

        assertFalse(jwtService.isAccessToken(jwtService.generateAccessToken(1L, "RIDER")));
        assertFalse(jwtService.isRefreshToken(jwtService.generateRefreshToken(1L, "RIDER")));
    }

    @Test
    @DisplayName("malformed, tampered and empty tokens are rejected without throwing")
    void malformedTokensRejected() {
        String access = jwtService.generateAccessToken(1L, "RIDER");

        assertTrue(jwtService.parseAccessToken("not-a-jwt").isEmpty());
        assertTrue(jwtService.parseAccessToken("a.b.c").isEmpty());
        assertTrue(jwtService.parseAccessToken("").isEmpty());
        assertTrue(jwtService.parseAccessToken(access.substring(0, access.length() - 2) + "xx").isEmpty());
        assertTrue(jwtService.parseRefreshToken("not-a-jwt").isEmpty());
    }

    @Test
    @DisplayName("correctly signed token WITHOUT a type claim (pre-upgrade token) is rejected")
    void tokenWithoutTypeRejected() {
        String legacy = Jwts.builder()
                .subject("1").claim("role", "ADMIN")
                .issuedAt(new Date()).expiration(new Date(System.currentTimeMillis() + 60_000))
                .signWith(Keys.hmacShaKeyFor(SECRET.getBytes(StandardCharsets.UTF_8)))
                .compact();

        assertTrue(jwtService.isTokenValid(legacy), "signature itself is valid");
        assertFalse(jwtService.isAccessToken(legacy));
        assertFalse(jwtService.isRefreshToken(legacy));
    }

    @Test
    @DisplayName("token signed with a different key is rejected")
    void foreignKeyRejected() {
        JwtService other = new JwtService();
        ReflectionTestUtils.setField(other, "secretKey", "anotherSecretKeyThatIsAlsoAtLeast32BytesLong!!");
        ReflectionTestUtils.setField(other, "accessTokenExpiry", 900000L);

        assertFalse(jwtService.isAccessToken(other.generateAccessToken(1L, "ADMIN")));
    }

    // ── SIGNING KEY CONFIGURATION ──

    private static JwtService serviceWith(String secret, boolean allowEphemeral) {
        JwtService service = new JwtService();
        ReflectionTestUtils.setField(service, "secretKey", secret);
        ReflectionTestUtils.setField(service, "allowEphemeralSecret", allowEphemeral);
        ReflectionTestUtils.setField(service, "accessTokenExpiry", 900000L);
        return service;
    }

    @Test
    @DisplayName("startup fails when JWT_SECRET is missing (no fallback secret)")
    void missingSecretFails() {
        assertThrows(IllegalStateException.class, () -> serviceWith("", false).validateConfiguration());
        assertThrows(IllegalStateException.class, () -> serviceWith(null, false).validateConfiguration());
    }

    @Test
    @DisplayName("startup fails when JWT_SECRET is shorter than 32 bytes")
    void shortSecretFails() {
        assertThrows(IllegalStateException.class,
                () -> serviceWith("only-31-bytes-long-secret-value", false).validateConfiguration());
    }

    @Test
    @DisplayName("startup fails for secrets published in this repository")
    void publishedSecretsFail() {
        assertThrows(IllegalStateException.class, () -> serviceWith(
                "myDefaultSecretKeyThatIsAtLeast32CharactersLongForHS256Algorithm", false).validateConfiguration());
        assertThrows(IllegalStateException.class, () -> serviceWith(
                "replace-with-a-long-random-secret", false).validateConfiguration());
    }

    @Test
    @DisplayName("local development without JWT_SECRET uses a random key that differs per process")
    void ephemeralKeyForLocalDevelopment() {
        JwtService first = serviceWith("", true);
        JwtService second = serviceWith("", true);
        first.validateConfiguration();
        second.validateConfiguration();

        String token = first.generateAccessToken(1L, "RIDER");
        assertTrue(first.isAccessToken(token));
        assertFalse(second.isAccessToken(token), "a random key must not be shared or predictable");
    }
}
