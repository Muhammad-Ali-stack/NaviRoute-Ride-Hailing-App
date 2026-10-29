package com.ridesharing.auth.config;

import com.ridesharing.auth.filter.JwtAuthenticationFilter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.autoconfigure.security.servlet.EndpointRequest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.ProviderManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.security.provisioning.InMemoryUserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;
import org.springframework.security.web.authentication.UsernamePasswordAuthenticationFilter;

/**
 * Configures Spring Security for the entire application.
 *
 * Key decisions:
 *   1. STATELESS sessions — no server-side session storage (JWT handles auth)
 *   2. CSRF disabled — not needed for stateless REST APIs (CSRF is for cookie-based auth)
 *   3. Public endpoints — /api/auth/** doesn't need a token (login, register, refresh)
 *   4. Role-based access — ADMIN endpoints only for ADMIN role
 *   5. JWT filter — our JwtAuthenticationFilter runs before Spring's default auth filter
 *
 * Two filter chains:
 *   - Actuator (/actuator/**): health is public; the Prometheus endpoint needs the
 *     metrics scrape user (HTTP Basic) or an ADMIN token; everything else needs ADMIN.
 *   - API (everything else): JWT only.
 *
 * Filter chain order (what happens to every request):
 *   Request → JwtAuthenticationFilter → Spring's auth filters → SecurityConfig rules → Controller
 */
@Configuration
@EnableWebSecurity
public class SecurityConfig {

    static final String METRICS_ROLE = "METRICS";
    static final int MIN_METRICS_PASSWORD_LENGTH = 12;

    private final JwtAuthenticationFilter jwtAuthenticationFilter;

    public SecurityConfig(JwtAuthenticationFilter jwtAuthenticationFilter) {
        this.jwtAuthenticationFilter = jwtAuthenticationFilter;
    }

    /**
     * Rules for Spring Boot Actuator endpoints.
     *
     * The Prometheus scrape user (NAVIROUTE_METRICS_USERNAME / NAVIROUTE_METRICS_PASSWORD) is
     * known ONLY to this chain: it can read /actuator/prometheus and nothing else, and it can
     * never authenticate against the /api endpoints. If it is not configured, HTTP Basic is
     * disabled and only ADMIN tokens can read metrics.
     */
    @Bean
    @Order(1)
    public SecurityFilterChain actuatorSecurityFilterChain(
            HttpSecurity http,
            @Value("${naviroute.metrics.username:}") String metricsUsername,
            @Value("${naviroute.metrics.password:}") String metricsPassword) throws Exception {

        http
                .securityMatcher(EndpointRequest.toAnyEndpoint())
                .csrf(csrf -> csrf.disable())
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))
                .authorizeHttpRequests(auth -> auth
                        // PUBLIC: health status and liveness/readiness probes (details only for ADMIN)
                        .requestMatchers(EndpointRequest.to("health")).permitAll()
                        // Prometheus scrape: scrape user or ADMIN
                        .requestMatchers(EndpointRequest.to("prometheus"))
                        .hasAnyRole(METRICS_ROLE, "ADMIN")
                        // Everything else (metrics, info, endpoint index): ADMIN only
                        .anyRequest().hasRole("ADMIN")
                )
                .addFilterBefore(jwtAuthenticationFilter, UsernamePasswordAuthenticationFilter.class);

        if (isMetricsUserConfigured(metricsUsername, metricsPassword)) {
            InMemoryUserDetailsManager scrapeUsers = new InMemoryUserDetailsManager(
                    org.springframework.security.core.userdetails.User
                            .withUsername(metricsUsername)
                            .password(passwordEncoder().encode(metricsPassword))
                            .roles(METRICS_ROLE)
                            .build());
            DaoAuthenticationProvider provider = new DaoAuthenticationProvider();
            provider.setUserDetailsService(scrapeUsers);
            provider.setPasswordEncoder(passwordEncoder());

            http.authenticationManager(new ProviderManager(provider))
                    .httpBasic(Customizer.withDefaults());
        } else {
            http.httpBasic(basic -> basic.disable());
        }

        return http.build();
    }

    /**
     * Defines the security filter chain for the API — the rules for who can access what.
     *
     * @param http the HttpSecurity builder
     * @return the configured SecurityFilterChain
     */
    @Bean
    @Order(2)
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                // Disable CSRF — not needed for stateless JWT-based REST APIs.
                // CSRF protection is for browser cookie-based sessions.
                // Since we use JWT in the Authorization header, CSRF doesn't apply.
                .csrf(csrf -> csrf.disable())

                // Set session management to STATELESS — no HttpSession created.
                // Every request is authenticated independently via JWT.
                // This is what makes our app horizontally scalable —
                // any server instance can handle any request.
                .sessionManagement(session ->
                        session.sessionCreationPolicy(SessionCreationPolicy.STATELESS))

                // Define endpoint access rules (ORDER MATTERS — first match wins)
                .authorizeHttpRequests(auth -> auth

                        // PUBLIC: auth endpoints (register, login, refresh) — no token needed
                        .requestMatchers("/api/auth/**").permitAll()

                        // PUBLIC: WebSocket endpoints — auth handled at WebSocket level
                        .requestMatchers("/ws/**").permitAll()

                        // ADMIN ONLY: admin management, payment refunds, fare-cache invalidation
                        .requestMatchers("/api/admin/**").hasRole("ADMIN")
                        .requestMatchers("/api/payments/*/refund").hasRole("ADMIN")
                        .requestMatchers(HttpMethod.DELETE, "/api/pricing/cache/**").hasRole("ADMIN")

                        // AUTHENTICATED: everything else requires a valid JWT
                        .anyRequest().authenticated()
                )

                // Insert our JWT filter BEFORE Spring's UsernamePasswordAuthenticationFilter.
                // This means our filter validates the JWT first, THEN Spring checks the rules above.
                .addFilterBefore(jwtAuthenticationFilter,
                        UsernamePasswordAuthenticationFilter.class);

        return http.build();
    }

    /**
     * The password encoder bean used throughout the application.
     * BCrypt is the industry standard for password hashing:
     *   - Automatically adds a random salt (two identical passwords produce different hashes)
     *   - Deliberately slow (prevents brute-force attacks)
     *   - Configurable strength (default 10 rounds = good balance of security and speed)
     *
     * @return BCryptPasswordEncoder instance
     */
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();
    }

    /** Both or neither must be set; a configured password must be reasonably long. */
    static boolean isMetricsUserConfigured(String username, String password) {
        boolean hasUser = username != null && !username.isBlank();
        boolean hasPassword = password != null && !password.isBlank();
        if (!hasUser && !hasPassword) {
            return false;
        }
        if (!hasUser || !hasPassword) {
            throw new IllegalStateException(
                    "Set both NAVIROUTE_METRICS_USERNAME and NAVIROUTE_METRICS_PASSWORD, or neither");
        }
        if (password.length() < MIN_METRICS_PASSWORD_LENGTH) {
            throw new IllegalStateException("NAVIROUTE_METRICS_PASSWORD must be at least "
                    + MIN_METRICS_PASSWORD_LENGTH + " characters");
        }
        return true;
    }
}
