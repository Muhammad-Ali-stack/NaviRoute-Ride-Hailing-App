package com.ridesharing.auth.service;

import com.ridesharing.auth.dto.AuthResponse;
import com.ridesharing.auth.dto.LoginRequest;
import com.ridesharing.auth.dto.RegisterRequest;
import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.shared.enums.DriverApprovalStatus;
import com.ridesharing.shared.enums.DriverAvailability;
import com.ridesharing.shared.enums.UserRole;
import com.ridesharing.shared.exceptions.ConflictException;
import com.ridesharing.shared.exceptions.UnauthorizedException;
import io.jsonwebtoken.Claims;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import java.util.Locale;
import java.util.Optional;
import java.util.UUID;

/**
 * Business logic for authentication: register, login, refresh tokens.
 * This is where ALL the auth decisions happen.
 *
 * Coordinates between:
 *   - UserRepository (database operations)
 *   - JwtService (token generation/validation)
 *   - PasswordEncoder (BCrypt hashing)
 *
 * Controller calls this service. This service NEVER touches HTTP directly.
 */
@Service
public class AuthService {

    private static final Logger log = LoggerFactory.getLogger(AuthService.class);

    /** One message for every login failure, so responses do not reveal whether an account exists. */
    static final String INVALID_CREDENTIALS = "Invalid email or password";
    static final String INVALID_REFRESH_TOKEN = "Invalid or expired refresh token";

    // Dependencies injected via constructor (not @Autowired on fields — better for testing)
    private final UserRepository userRepository;
    private final JwtService jwtService;
    private final PasswordEncoder passwordEncoder;

    private volatile String dummyPasswordHash;

    /**
     * Constructor injection — Spring automatically provides these dependencies.
     * We use constructor injection (not @Autowired on fields) because:
     *   1. Makes dependencies explicit — you see exactly what this class needs
     *   2. Makes testing easier — you can pass mock objects in tests
     *   3. Ensures the object is fully initialized before use
     */
    public AuthService(UserRepository userRepository, JwtService jwtService,
                       PasswordEncoder passwordEncoder) {
        this.userRepository = userRepository;
        this.jwtService = jwtService;
        this.passwordEncoder = passwordEncoder;
    }

    /**
     * Registers a new user (Rider or Driver) in the system.
     *
     * Steps:
     *   1. Check if email already exists → throw ConflictException if yes
     *   2. Hash the password with BCrypt
     *   3. Prevent self-registration as ADMIN (security measure)
     *   4. Save user to PostgreSQL
     *   5. Generate JWT tokens
     *   6. Return tokens to client
     *
     * @param request registration data from the client
     * @return AuthResponse with access token, refresh token, and role
     * @throws ConflictException if email is already registered
     */
    public AuthResponse register(RegisterRequest request) {
        // Email identity is case-insensitive; new accounts are stored in lower case
        String email = normalizeEmail(request.getEmail());
        log.info("Registration attempt for email: {}", email);

        // Step 1: Check for duplicate email (ignoring letter case)
        if (userRepository.existsByEmail(email)) {
            log.warn("Registration failed — email already exists: {}", email);
            throw new ConflictException("Email already registered: " + email);
        }

        // Step 2: Prevent self-registration as ADMIN
        if (request.getRole() == UserRole.ADMIN) {
            log.warn("Attempt to self-register as ADMIN blocked: {}", request.getEmail());
            throw new UnauthorizedException("Cannot self-register as ADMIN");
        }

        // Step 3: Build the User entity with hashed password
        User user = User.builder()
                .name(request.getName())
                .email(email)
                .password(passwordEncoder.encode(request.getPassword()))
                .phone(request.getPhone())
                .role(request.getRole())
                .active(true)
                .build();

        // New drivers must be reviewed by an admin before they can go online
        if (request.getRole() == UserRole.DRIVER) {
            user.setApprovalStatus(DriverApprovalStatus.PENDING);
            user.setAvailability(DriverAvailability.OFFLINE);
        }

        // Step 4: Save to PostgreSQL. A concurrent registration of the same email (in any
        // letter case) that slipped past Step 1 is rejected by the uq_users_email_lower index.
        User savedUser;
        try {
            savedUser = userRepository.save(user);
        } catch (DataIntegrityViolationException ex) {
            log.warn("Registration failed — email registered concurrently: {}", email);
            throw new ConflictException("Email already registered: " + email);
        }
        log.info("User registered successfully: id={}, role={}", savedUser.getId(), savedUser.getRole());

        // Step 5: Generate tokens and return
        return generateAuthResponse(savedUser);
    }

    /**
     * Authenticates a user by email and password.
     *
     * Steps:
     *   1. Find user by email (case-insensitive)
     *   2. Compare provided password with stored BCrypt hash
     *   3. Check if account is active → throw UnauthorizedException if suspended
     *   4. Generate JWT tokens
     *   5. Return tokens to client
     *
     * Unknown email and wrong password produce the identical 401 "Invalid email or password".
     *
     * @param request login credentials from the client
     * @return AuthResponse with access token, refresh token, and role
     * @throws UnauthorizedException if the credentials are invalid or the account is suspended
     */
    public AuthResponse login(LoginRequest request) {
        String email = normalizeEmail(request.getEmail());
        log.info("Login attempt for email: {}", email);

        // Step 1: Find user by email (case-insensitive)
        Optional<User> found = userRepository.findByEmail(email);
        if (found.isEmpty()) {
            // Same 401 + message as a wrong password, and the same BCrypt work,
            // so neither the response nor its timing reveals whether the account exists
            passwordEncoder.matches(request.getPassword(), dummyPasswordHash());
            log.warn("Login failed — unknown email: {}", email);
            throw new UnauthorizedException(INVALID_CREDENTIALS);
        }
        User user = found.get();

        // Step 2: Verify password
        // passwordEncoder.matches() compares raw password with BCrypt hash
        // It returns true/false — we never decrypt the hash (that's impossible with BCrypt)
        if (!passwordEncoder.matches(request.getPassword(), user.getPassword())) {
            log.warn("Login failed — wrong password for email: {}", email);
            throw new UnauthorizedException(INVALID_CREDENTIALS);
        }

        // Step 3: Check if account is active. Only reported AFTER a correct password,
        // so account status cannot be probed without knowing the password.
        if (!user.isActive()) {
            log.warn("Login failed — account suspended: {}", email);
            throw new UnauthorizedException("Account is suspended. Contact support.");
        }

        log.info("Login successful: userId={}, role={}", user.getId(), user.getRole());

        // Step 4: Generate tokens and return
        return generateAuthResponse(user);
    }

    /**
     * Creates the AuthResponse with fresh JWT tokens.
     * Both register and login use this to avoid code duplication.
     *
     * @param user the authenticated/registered user
     * @return AuthResponse containing tokens and role
     */
    /**
     * Exchanges a valid REFRESH token for a new access + refresh token pair.
     *
     * Rejected (401, same generic message for every case) when the token is malformed,
     * expired, signed with another key, is an ACCESS token, or the account no longer
     * exists or is suspended. The role in the new tokens is read from the database.
     */
    public AuthResponse refresh(String refreshToken) {
        Claims claims = jwtService.parseRefreshToken(refreshToken)
                .orElseThrow(() -> new UnauthorizedException(INVALID_REFRESH_TOKEN));

        Long userId;
        try {
            userId = Long.valueOf(claims.getSubject());
        } catch (NumberFormatException ex) {
            throw new UnauthorizedException(INVALID_REFRESH_TOKEN);
        }

        User user = userRepository.findById(userId)
                .filter(User::isActive)
                .orElseThrow(() -> new UnauthorizedException(INVALID_REFRESH_TOKEN));

        log.info("Tokens refreshed for userId={}", userId);
        return generateAuthResponse(user);
    }

    /** Trims surrounding whitespace and lower-cases (locale-independent); nothing else is altered. */
    static String normalizeEmail(String email) {
        return email == null ? null : email.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * BCrypt hash of a random value, used to spend the same hashing time when the email is
     * unknown — so response timing does not reveal whether an account exists.
     */
    private String dummyPasswordHash() {
        String hash = dummyPasswordHash;
        if (hash == null) {
            hash = passwordEncoder.encode(UUID.randomUUID().toString());
            dummyPasswordHash = hash;
        }
        return hash;
    }

    private AuthResponse generateAuthResponse(User user) {
        String accessToken = jwtService.generateAccessToken(user.getId(), user.getRole().name());
        String refreshToken = jwtService.generateRefreshToken(user.getId(), user.getRole().name());

        return AuthResponse.builder()
                .accessToken(accessToken)
                .refreshToken(refreshToken)
                .role(user.getRole())
                .build();
    }
}
