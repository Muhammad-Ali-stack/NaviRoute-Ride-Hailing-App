package com.ridesharing.auth.service;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.MalformedJwtException;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Date;
import java.util.HexFormat;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * Handles ALL JWT operations for the application:
 *   - Generate access tokens (short-lived: 15 minutes)
 *   - Generate refresh tokens (long-lived: 7 days)
 *   - Validate tokens (not expired, not tampered, correct token type)
 *   - Extract claims (userId, role) from tokens
 *
 * A JWT looks like: xxxxx.yyyyy.zzzzz
 *   Part 1 (xxxxx): Header — {"alg": "HS512", "typ": "JWT"}
 *   Part 2 (yyyyy): Payload — {"sub": "42", "role": "RIDER", "type": "access", "exp": 1711680000}
 *   Part 3 (zzzzz): Signature — HMAC-SHA(part1 + "." + part2, SECRET_KEY)
 *
 * The signature ensures nobody can tamper with the payload.
 * If they change "RIDER" to "ADMIN", the signature won't match and we reject it.
 *
 * Token types: every token carries a signed "type" claim ("access" or "refresh").
 * Only access tokens authenticate API requests; only refresh tokens can be exchanged
 * at /api/auth/refresh. Tokens without a valid type claim are rejected.
 */
@Service
public class JwtService {

    private static final Logger log = LoggerFactory.getLogger(JwtService.class);

    public static final String TOKEN_TYPE_CLAIM = "type";

    /** The two kinds of token this service issues; the value is stored in the "type" claim. */
    public enum TokenType {
        ACCESS("access"),
        REFRESH("refresh");

        private final String claimValue;

        TokenType(String claimValue) {
            this.claimValue = claimValue;
        }

        public String claimValue() {
            return claimValue;
        }
    }

    /** HMAC-SHA256 needs a key of at least 256 bits. */
    static final int MIN_SECRET_BYTES = 32;

    /**
     * SHA-256 digests of secrets that are published in this repository (the old development
     * default and the .env.example placeholder). They must never be used to sign tokens.
     */
    private static final Set<String> KNOWN_INSECURE_SECRET_DIGESTS = Set.of(
            "d5bf03e64c483173424831fb40f506e30e636e9770850c2776b0a4560c991235",
            "79ef599b03c84b902bc2c21405ad47203ca5606291b87fc383219ccc0c9d77f6");

    // Secret key for signing tokens — set JWT_SECRET as an environment variable.
    // There is NO built-in default: without it the application refuses to start, unless
    // jwt.allow-ephemeral-secret is enabled (local development profile only).
    @Value("${jwt.secret:}")
    private String secretKey;

    // Local development only: when JWT_SECRET is unset, sign with a random per-process key
    // instead of failing. Tokens then stop working after a restart.
    @Value("${jwt.allow-ephemeral-secret:false}")
    private boolean allowEphemeralSecret;

    // Access token expiry: 15 minutes (in milliseconds).
    // Short-lived because if stolen, the damage window is only 15 minutes.
    @Value("${jwt.access-token-expiry:900000}")
    private long accessTokenExpiry;

    // Refresh token expiry: 7 days (in milliseconds).
    // Used to get a new access token without re-entering password.
    @Value("${jwt.refresh-token-expiry:604800000}")
    private long refreshTokenExpiry;

    private volatile SecretKey signingKey;

    /** Fail fast at startup if the signing key is missing or insecure. */
    @PostConstruct
    void validateConfiguration() {
        getSigningKey();
    }

    /**
     * Generates a short-lived access token for a user.
     * This token is sent with every API request in the Authorization header.
     *
     * @param userId the user's database ID
     * @param role the user's role (RIDER, DRIVER, ADMIN)
     * @return signed JWT string
     */
    public String generateAccessToken(Long userId, String role) {
        return buildToken(userId, role, TokenType.ACCESS, accessTokenExpiry);
    }

    /**
     * Generates a long-lived refresh token.
     * It can only be exchanged for new tokens at /api/auth/refresh — it is rejected
     * as an API credential.
     */
    public String generateRefreshToken(Long userId, String role) {
        return buildToken(userId, role, TokenType.REFRESH, refreshTokenExpiry);
    }

    /**
     * Validates signature, expiry and token type "access".
     * Use this for authenticating API requests.
     */
    public Optional<Claims> parseAccessToken(String token) {
        return parseToken(token, TokenType.ACCESS);
    }

    /**
     * Validates signature, expiry and token type "refresh".
     * Use this only for the refresh endpoint.
     */
    public Optional<Claims> parseRefreshToken(String token) {
        return parseToken(token, TokenType.REFRESH);
    }

    public boolean isAccessToken(String token) {
        return parseAccessToken(token).isPresent();
    }

    public boolean isRefreshToken(String token) {
        return parseRefreshToken(token).isPresent();
    }

    /**
     * Extracts the user ID from a token's "subject" field.
     */
    public Long extractUserId(String token) {
        String subject = extractClaim(token, Claims::getSubject);
        return Long.parseLong(subject);
    }

    /**
     * Extracts the user's role from the custom "role" claim.
     */
    public String extractRole(String token) {
        return extractClaim(token, claims -> claims.get("role", String.class));
    }

    /**
     * Checks signature and expiry only — it does NOT check the token type.
     * For authenticating requests use {@link #parseAccessToken(String)}.
     */
    public boolean isTokenValid(String token) {
        return verifiedClaims(token).isPresent();
    }

    /** Verifies signature and expiry; logs and returns empty for any invalid token. */
    private Optional<Claims> verifiedClaims(String token) {
        try {
            return Optional.of(extractAllClaims(token));
        } catch (ExpiredJwtException ex) {
            log.warn("JWT token expired: {}", ex.getMessage());
        } catch (MalformedJwtException ex) {
            log.warn("JWT token malformed: {}", ex.getMessage());
        } catch (SignatureException ex) {
            log.warn("JWT signature invalid: {}", ex.getMessage());
        } catch (Exception ex) {
            log.warn("JWT token invalid: {}", ex.getMessage());
        }
        return Optional.empty();
    }

    private Optional<Claims> parseToken(String token, TokenType expectedType) {
        Optional<Claims> verified = verifiedClaims(token);
        if (verified.isEmpty()) {
            return Optional.empty();
        }
        Claims claims = verified.get();
        String type = claims.get(TOKEN_TYPE_CLAIM, String.class);
        if (!expectedType.claimValue().equals(type)) {
            log.warn("JWT rejected: expected {} token but got type={}", expectedType.claimValue(), type);
            return Optional.empty();
        }
        return Optional.of(claims);
    }

    private String buildToken(Long userId, String role, TokenType type, long expiryMs) {
        Date now = new Date();
        Date expiry = new Date(now.getTime() + expiryMs);

        return Jwts.builder()
                .subject(String.valueOf(userId))
                .claim("role", role)
                .claim(TOKEN_TYPE_CLAIM, type.claimValue())
                .issuedAt(now)
                .expiration(expiry)
                .signWith(getSigningKey())
                .compact();
    }

    private <T> T extractClaim(String token, Function<Claims, T> claimsResolver) {
        Claims claims = extractAllClaims(token);
        return claimsResolver.apply(claims);
    }

    private Claims extractAllClaims(String token) {
        return Jwts.parser()
                .verifyWith(getSigningKey())
                .build()
                .parseSignedClaims(token)
                .getPayload();
    }

    private SecretKey getSigningKey() {
        SecretKey key = signingKey;
        if (key == null) {
            synchronized (this) {
                if (signingKey == null) {
                    signingKey = createSigningKey();
                }
                key = signingKey;
            }
        }
        return key;
    }

    private SecretKey createSigningKey() {
        if (secretKey == null || secretKey.isBlank()) {
            if (allowEphemeralSecret) {
                log.warn("JWT_SECRET is not set — signing tokens with a random key for this process only. "
                        + "Tokens stop working after a restart. Never rely on this outside local development.");
                byte[] random = new byte[64];
                new SecureRandom().nextBytes(random);
                return Keys.hmacShaKeyFor(random);
            }
            throw new IllegalStateException(
                    "JWT_SECRET (jwt.secret) must be set to a random value of at least " + MIN_SECRET_BYTES + " bytes");
        }

        byte[] keyBytes = secretKey.getBytes(StandardCharsets.UTF_8);
        if (keyBytes.length < MIN_SECRET_BYTES) {
            throw new IllegalStateException("JWT_SECRET (jwt.secret) is too short: it must be at least "
                    + MIN_SECRET_BYTES + " bytes");
        }
        if (KNOWN_INSECURE_SECRET_DIGESTS.contains(sha256Hex(keyBytes))) {
            throw new IllegalStateException("JWT_SECRET (jwt.secret) uses a value published in this repository; "
                    + "generate a new random secret");
        }
        return Keys.hmacShaKeyFor(keyBytes);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 not available", ex);
        }
    }
}
