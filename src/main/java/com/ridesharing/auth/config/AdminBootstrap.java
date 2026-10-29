package com.ridesharing.auth.config;

import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.shared.enums.UserRole;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * Creates (or repairs) an administrator account from configuration at startup.
 *
 * Configure with NAVIROUTE_ADMIN_EMAIL and NAVIROUTE_ADMIN_PASSWORD. If neither is set,
 * nothing happens. There is no built-in default password.
 *
 * Behaviour:
 *   - No account with that email → a new ADMIN account is created.
 *   - Existing ADMIN that still has the unusable password hash seeded by migration V7
 *     → its password is replaced and it is re-activated (one-time repair; afterwards
 *     the hash no longer matches, so later startups leave it alone).
 *   - Any other existing ADMIN → left unchanged (credentials are never reset on restart).
 *   - Existing non-admin account → startup fails rather than silently granting ADMIN.
 *
 * The password is never logged.
 */
@Component
public class AdminBootstrap implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(AdminBootstrap.class);

    /** Password hash inserted by V7__seed_admin_user.sql; it does not match the documented password. */
    static final String V7_SEED_PASSWORD_HASH =
            "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy";

    static final int MIN_PASSWORD_LENGTH = 12;
    private static final Pattern EMAIL_PATTERN = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final String email;
    private final String password;

    public AdminBootstrap(UserRepository userRepository,
                          PasswordEncoder passwordEncoder,
                          @Value("${naviroute.admin.email:}") String email,
                          @Value("${naviroute.admin.password:}") String password) {
        this.userRepository = userRepository;
        this.passwordEncoder = passwordEncoder;
        // Same normalization as registration/login: email identity is case-insensitive
        this.email = email == null ? "" : email.trim().toLowerCase(Locale.ROOT);
        this.password = password == null ? "" : password;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (email.isEmpty() && password.isEmpty()) {
            log.info("Admin bootstrap not configured (NAVIROUTE_ADMIN_EMAIL / NAVIROUTE_ADMIN_PASSWORD unset)");
            return;
        }
        validateConfiguration();

        Optional<User> existing = userRepository.findByEmail(email);
        if (existing.isEmpty()) {
            createAdmin();
            return;
        }

        User user = existing.get();
        if (user.getRole() != UserRole.ADMIN) {
            throw new IllegalStateException("Admin bootstrap: account " + email
                    + " exists but is not an ADMIN — refusing to change its role");
        }

        if (V7_SEED_PASSWORD_HASH.equals(user.getPassword())) {
            user.setPassword(passwordEncoder.encode(password));
            user.setActive(true);
            userRepository.save(user);
            log.info("Admin bootstrap: replaced the unusable seeded password for administrator {}", email);
            return;
        }

        log.info("Admin bootstrap: administrator {} already exists — credentials left unchanged", email);
    }

    private void validateConfiguration() {
        if (email.isEmpty() || password.isEmpty()) {
            throw new IllegalStateException(
                    "Admin bootstrap: set both NAVIROUTE_ADMIN_EMAIL and NAVIROUTE_ADMIN_PASSWORD, or neither");
        }
        if (!EMAIL_PATTERN.matcher(email).matches()) {
            throw new IllegalStateException("Admin bootstrap: NAVIROUTE_ADMIN_EMAIL is not a valid email address");
        }
        if (password.length() < MIN_PASSWORD_LENGTH || password.isBlank()) {
            throw new IllegalStateException("Admin bootstrap: NAVIROUTE_ADMIN_PASSWORD must be at least "
                    + MIN_PASSWORD_LENGTH + " characters");
        }
    }

    private void createAdmin() {
        User admin = User.builder()
                .name("Platform Admin")
                .email(email)
                .password(passwordEncoder.encode(password))
                .phone("0000000000")
                .role(UserRole.ADMIN)
                .active(true)
                .build();
        userRepository.save(admin);
        log.info("Admin bootstrap: created administrator account {}", email);
    }
}
