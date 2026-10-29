package com.ridesharing.auth;

import com.fasterxml.jackson.databind.JsonNode;
import com.ridesharing.auth.dto.RegisterRequest;
import com.ridesharing.auth.service.AuthService;
import com.ridesharing.shared.enums.UserRole;
import com.ridesharing.shared.exceptions.ConflictException;
import com.ridesharing.support.IntegrationTestBase;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MvcResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

/**
 * Token types, suspension revocation and email identity — exercised through the real
 * HTTP security filter chain against embedded PostgreSQL.
 */
class AuthSecurityIntegrationTest extends IntegrationTestBase {

    @Autowired
    private AuthService authService;

    private JsonNode loginBody(String email, String password) throws Exception {
        return body(expectStatus(200, perform(post("/api/auth/login"), null,
                Map.of("email", email, "password", password)))).get("data");
    }

    // ── 1. Token types ──

    @Test
    @DisplayName("Access token authenticates API calls; refresh token does not")
    void refreshTokenIsNotAnApiCredential() throws Exception {
        String email = uniqueEmail("tokens");
        register(email, "RIDER");
        JsonNode tokens = loginBody(email, "pass1234");

        expectStatus(200, perform(get("/api/rides/history/rider"), tokens.get("accessToken").asText(), null));
        expectStatus(403, perform(get("/api/rides/history/rider"), tokens.get("refreshToken").asText(), null));
    }

    @Test
    @DisplayName("Refresh endpoint accepts refresh tokens only; access, malformed and empty tokens get 401")
    void accessTokenIsNotARefreshToken() throws Exception {
        String email = uniqueEmail("refresh");
        register(email, "RIDER");
        JsonNode tokens = loginBody(email, "pass1234");

        JsonNode refreshed = body(expectStatus(200, perform(post("/api/auth/refresh"), null,
                Map.of("refreshToken", tokens.get("refreshToken").asText())))).get("data");
        expectStatus(200, perform(get("/api/rides/history/rider"), refreshed.get("accessToken").asText(), null));

        MvcResult withAccess = expectStatus(401, perform(post("/api/auth/refresh"), null,
                Map.of("refreshToken", tokens.get("accessToken").asText())));
        assertEquals("Invalid or expired refresh token", body(withAccess).get("message").asText());
        expectStatus(401, perform(post("/api/auth/refresh"), null, Map.of("refreshToken", "not.a.jwt")));
        expectStatus(400, perform(post("/api/auth/refresh"), null, Map.of("refreshToken", "")));
    }

    // ── 2. Suspension revokes existing tokens ──

    @Test
    @DisplayName("Token issued before suspension stops working; other users' tokens keep working")
    void suspensionRevokesExistingTokens() throws Exception {
        String suspendedEmail = uniqueEmail("suspended");
        String activeEmail = uniqueEmail("active");
        register(suspendedEmail, "RIDER");
        register(activeEmail, "RIDER");
        JsonNode suspendedTokens = loginBody(suspendedEmail, "pass1234");
        String suspendedAccess = suspendedTokens.get("accessToken").asText();
        String activeAccess = loginBody(activeEmail, "pass1234").get("accessToken").asText();

        expectStatus(200, perform(get("/api/payments/history/rider"), suspendedAccess, null));

        expectStatus(200, perform(put("/api/admin/users/" + userId(suspendedEmail) + "/suspend"), adminToken(), null));

        expectStatus(403, perform(get("/api/payments/history/rider"), suspendedAccess, null));
        expectStatus(401, perform(post("/api/auth/refresh"), null,
                Map.of("refreshToken", suspendedTokens.get("refreshToken").asText())));
        expectStatus(200, perform(get("/api/payments/history/rider"), activeAccess, null));

        // Reactivation restores access for still-unexpired tokens (status is checked per request)
        expectStatus(200, perform(put("/api/admin/users/" + userId(suspendedEmail) + "/reactivate"), adminToken(), null));
        expectStatus(200, perform(get("/api/payments/history/rider"), suspendedAccess, null));
    }

    // ── 7. Email identity ──

    @Test
    @DisplayName("Emails differing only by case cannot be registered twice; login is case-insensitive")
    void caseInsensitiveEmailIdentity() throws Exception {
        String local = "Case.Test-" + UUID.randomUUID();
        register(local + "@NaviRoute.Test", "RIDER");

        assertEquals(local.toLowerCase() + "@naviroute.test",
                jdbc.queryForObject("SELECT email FROM users WHERE LOWER(email) = LOWER(?)", String.class,
                        local + "@naviroute.test"));

        expectStatus(409, perform(post("/api/auth/register"), null, Map.of(
                "name", "Dup", "email", local.toUpperCase() + "@NAVIROUTE.TEST",
                "password", "pass1234", "phone", "9000000000", "role", "RIDER")));

        assertFalse(loginBody(local.toUpperCase() + "@naviroute.TEST", "pass1234")
                .get("accessToken").asText().isBlank());
    }

    @Test
    @DisplayName("Unknown email and wrong password produce the identical 401 response")
    void genericLoginFailure() throws Exception {
        String email = uniqueEmail("generic");
        register(email, "RIDER");

        MvcResult unknown = perform(post("/api/auth/login"), null,
                Map.of("email", uniqueEmail("nobody"), "password", "pass1234"));
        MvcResult wrongPassword = perform(post("/api/auth/login"), null,
                Map.of("email", email, "password", "wrong-password"));

        assertEquals(401, unknown.getResponse().getStatus());
        assertEquals(401, wrongPassword.getResponse().getStatus());
        assertEquals(body(wrongPassword).get("message").asText(), body(unknown).get("message").asText());
        assertEquals("Invalid email or password", body(unknown).get("message").asText());
    }

    @Test
    @DisplayName("Concurrent registrations of one email in different letter cases: exactly one succeeds")
    void concurrentRegistrationSameEmail() throws Exception {
        String local = "race-" + UUID.randomUUID();
        List<String> variants = List.of(local + "@naviroute.test", local.toUpperCase() + "@NAVIROUTE.TEST",
                local + "@NaviRoute.Test", local.toUpperCase() + "@naviroute.test");

        CyclicBarrier start = new CyclicBarrier(variants.size());
        ExecutorService pool = Executors.newFixedThreadPool(variants.size());
        List<Future<Boolean>> results = new ArrayList<>();
        try {
            for (String email : variants) {
                results.add(pool.submit(() -> {
                    start.await();
                    try {
                        authService.register(RegisterRequest.builder().name("Racer").email(email)
                                .password("pass1234").phone("9000000000").role(UserRole.RIDER).build());
                        return true;
                    } catch (ConflictException ex) {
                        return false;
                    }
                }));
            }
            int successes = 0;
            for (Future<Boolean> result : results) {
                if (result.get(30, TimeUnit.SECONDS)) {
                    successes++;
                }
            }
            assertEquals(1, successes);
        } finally {
            pool.shutdownNow();
        }

        assertEquals(1, jdbc.queryForObject("SELECT COUNT(*) FROM users WHERE LOWER(email) = LOWER(?)",
                Integer.class, local + "@naviroute.test"));
    }
}
