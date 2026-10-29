package com.ridesharing.support;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import redis.embedded.RedisServer;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.UUID;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;

/**
 * Base class for integration tests that run the full application against REAL
 * infrastructure processes: embedded PostgreSQL (Flyway migrations V1+ applied, Hibernate
 * schema validation on), an embedded Kafka broker and an embedded Redis server.
 * Nothing on the persistence / messaging / cache path is mocked.
 *
 * All subclasses share this exact configuration, so Spring caches one application context
 * and the infrastructure is started once per test JVM.
 */
@SpringBootTest(properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "naviroute.admin.email=" + IntegrationTestBase.ADMIN_EMAIL,
        "naviroute.admin.password=" + IntegrationTestBase.ADMIN_PASSWORD,
        "naviroute.matching.retry-delay-ms=200",
        "spring.jpa.show-sql=false",
        "logging.level.com.ridesharing=INFO",
        "logging.level.org.springframework.web=INFO",
        "logging.level.org.hibernate.SQL=WARN"
})
@AutoConfigureMockMvc
@EmbeddedKafka(partitions = 1, topics = {
        "ride-requested", "ride-accepted", "ride-completed", "ride-cancelled", "driver-approved"})
public abstract class IntegrationTestBase {

    protected static final String ADMIN_EMAIL = "it-admin@naviroute.test";
    protected static final String ADMIN_PASSWORD = "integration-test-admin-password";

    // Pickup/dropoff used by ride tests (~5 km apart)
    protected static final double PICKUP_LAT = 12.9716;
    protected static final double PICKUP_LNG = 77.5946;
    protected static final double DROPOFF_LAT = 12.9352;
    protected static final double DROPOFF_LNG = 77.6245;

    private static EmbeddedPostgres postgres;
    private static RedisServer redisServer;
    private static int redisPort;

    @Autowired
    protected MockMvc mvc;

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected RedisTemplate<String, Object> redisTemplate;

    protected final ObjectMapper json = new ObjectMapper();

    @DynamicPropertySource
    static void infrastructure(DynamicPropertyRegistry registry) throws IOException {
        startInfrastructureOnce();
        registry.add("spring.datasource.url", () -> postgres.getJdbcUrl("postgres", "postgres"));
        registry.add("spring.datasource.username", () -> "postgres");
        registry.add("spring.datasource.password", () -> "postgres");
        registry.add("spring.data.redis.host", () -> "localhost");
        registry.add("spring.data.redis.port", () -> redisPort);
    }

    private static synchronized void startInfrastructureOnce() throws IOException {
        if (postgres == null) {
            postgres = EmbeddedPostgres.builder().start();
            redisPort = freePort();
            redisServer = new RedisServer(redisPort);
            redisServer.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    redisServer.stop();
                    postgres.close();
                } catch (IOException ignored) {
                    // best effort on JVM exit
                }
            }));
        }
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** Each test starts with an empty Redis (no driver locations, cache or surge from other tests). */
    @BeforeEach
    void flushRedis() {
        redisTemplate.getConnectionFactory().getConnection().serverCommands().flushAll();
    }

    // ── HTTP helpers ──

    protected MvcResult perform(MockHttpServletRequestBuilder request, String token, Object body) throws Exception {
        if (token != null) {
            request.header("Authorization", "Bearer " + token);
        }
        if (body != null) {
            request.contentType("application/json").content(json.writeValueAsString(body));
        }
        return mvc.perform(request).andReturn();
    }

    protected JsonNode body(MvcResult result) throws Exception {
        return json.readTree(result.getResponse().getContentAsString());
    }

    protected static String uniqueEmail(String prefix) {
        return prefix + "-" + UUID.randomUUID() + "@naviroute.test";
    }

    protected String register(String email, String role) throws Exception {
        MvcResult result = perform(post("/api/auth/register"), null, java.util.Map.of(
                "name", role + " " + email.substring(0, 8), "email", email,
                "password", "pass1234", "phone", "9000000000", "role", role));
        if (result.getResponse().getStatus() != 201) {
            throw new AssertionError("register failed: " + result.getResponse().getContentAsString());
        }
        return body(result).at("/data/accessToken").asText();
    }

    protected String login(String email, String password) throws Exception {
        MvcResult result = perform(post("/api/auth/login"), null, java.util.Map.of("email", email, "password", password));
        if (result.getResponse().getStatus() != 200) {
            throw new AssertionError("login failed (" + result.getResponse().getStatus() + "): "
                    + result.getResponse().getContentAsString());
        }
        return body(result).at("/data/accessToken").asText();
    }

    protected String adminToken() throws Exception {
        return login(ADMIN_EMAIL, ADMIN_PASSWORD);
    }

    protected Long userId(String email) {
        return jdbc.queryForObject("SELECT id FROM users WHERE email = ?", Long.class, email);
    }

    /**
     * Registers a driver with a vehicle, approves them as admin and puts them ONLINE
     * at the given position, all through the HTTP API. Returns the driver's token.
     */
    protected String onlineDriver(String email, String vehicleType, double lat, double lng) throws Exception {
        String token = register(email, "DRIVER");
        expectStatus(201, perform(post("/api/driver/vehicle"), token, java.util.Map.of(
                "vehicleType", vehicleType, "plateNumber", "P-" + UUID.randomUUID().toString().substring(0, 12),
                "model", "Model", "color", "White")));
        expectStatus(200, perform(put("/api/admin/drivers/" + userId(email) + "/approve"), adminToken(), null));
        expectStatus(200, perform(put("/api/driver/availability?status=ONLINE&latitude=" + lat + "&longitude=" + lng),
                token, null));
        return token;
    }

    protected static MvcResult expectStatus(int expected, MvcResult result) throws Exception {
        int actual = result.getResponse().getStatus();
        if (actual != expected) {
            throw new AssertionError("Expected HTTP " + expected + " but got " + actual + ": "
                    + result.getResponse().getContentAsString());
        }
        return result;
    }
}
