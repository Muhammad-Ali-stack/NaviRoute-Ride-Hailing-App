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
import io.jsonwebtoken.Jwts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for AuthService — register and login business logic.
 *
 * Uses Mockito to FAKE (mock) all dependencies:
 *   - UserRepository → fake database (no real PostgreSQL needed)
 *   - JwtService → fake token generation (returns predictable strings)
 *   - PasswordEncoder → fake hashing (returns predictable hashes)
 *
 * This makes tests:
 *   - FAST (no DB, no network)
 *   - RELIABLE (no flaky connections)
 *   - FOCUSED (testing only AuthService logic, not JPA or BCrypt)
 *
 * Happy path: register/login with valid data
 * Failure 1: register with duplicate email → ConflictException
 * Failure 2: login with wrong password → UnauthorizedException
 * Failure 3: login with non-existent email → same UnauthorizedException as a wrong password
 * Edge case: register as ADMIN → blocked
 */
@ExtendWith(MockitoExtension.class)
@DisplayName("AuthService Tests")
class AuthServiceTest {

    @Mock
    private UserRepository userRepository;

    @Mock
    private JwtService jwtService;

    @Mock
    private PasswordEncoder passwordEncoder;

    @InjectMocks
    private AuthService authService;

    private RegisterRequest registerRequest;
    private LoginRequest loginRequest;
    private User savedUser;

    @BeforeEach
    void setUp() {
        registerRequest = RegisterRequest.builder()
                .name("Raj Kumar")
                .email("raj@example.com")
                .password("securePass123")
                .phone("9876543210")
                .role(UserRole.RIDER)
                .build();

        loginRequest = LoginRequest.builder()
                .email("raj@example.com")
                .password("securePass123")
                .build();

        savedUser = User.builder()
                .id(1L)
                .name("Raj Kumar")
                .email("raj@example.com")
                .password("$2a$10$hashedPassword")
                .phone("9876543210")
                .role(UserRole.RIDER)
                .active(true)
                .build();
    }

    // ── REGISTER TESTS ──

    @Test
    @DisplayName("should_registerSuccessfully_when_validDataProvided")
    void should_registerSuccessfully_when_validDataProvided() {
        // GIVEN: email doesn't exist, password gets hashed, user gets saved
        when(userRepository.existsByEmail("raj@example.com")).thenReturn(false);
        when(passwordEncoder.encode("securePass123")).thenReturn("$2a$10$hashedPassword");
        when(userRepository.save(any(User.class))).thenReturn(savedUser);
        when(jwtService.generateAccessToken(1L, "RIDER")).thenReturn("access-token");
        when(jwtService.generateRefreshToken(1L, "RIDER")).thenReturn("refresh-token");

        // WHEN
        AuthResponse response = authService.register(registerRequest);

        // THEN
        assertNotNull(response);
        assertEquals("access-token", response.getAccessToken());
        assertEquals("refresh-token", response.getRefreshToken());
        assertEquals(UserRole.RIDER, response.getRole());
        verify(userRepository).save(any(User.class));
    }

    @Test
    @DisplayName("should_registerDriverAsPendingAndOffline_when_roleIsDriver")
    void should_registerDriverAsPendingAndOffline_when_roleIsDriver() {
        // GIVEN: a driver registration
        registerRequest.setRole(UserRole.DRIVER);
        when(userRepository.existsByEmail("raj@example.com")).thenReturn(false);
        when(passwordEncoder.encode("securePass123")).thenReturn("$2a$10$hashedPassword");
        when(userRepository.save(any(User.class))).thenAnswer(inv -> {
            User u = inv.getArgument(0);
            u.setId(5L);
            return u;
        });

        // WHEN
        authService.register(registerRequest);

        // THEN: the saved driver awaits admin review and is not online
        ArgumentCaptor<User> captor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(captor.capture());
        assertEquals(DriverApprovalStatus.PENDING, captor.getValue().getApprovalStatus());
        assertEquals(DriverAvailability.OFFLINE, captor.getValue().getAvailability());
    }

    @Test
    @DisplayName("should_notSetDriverFields_when_roleIsRider")
    void should_notSetDriverFields_when_roleIsRider() {
        when(userRepository.existsByEmail("raj@example.com")).thenReturn(false);
        when(passwordEncoder.encode("securePass123")).thenReturn("$2a$10$hashedPassword");
        when(userRepository.save(any(User.class))).thenReturn(savedUser);

        authService.register(registerRequest);

        ArgumentCaptor<User> captor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(captor.capture());
        assertNull(captor.getValue().getApprovalStatus());
        assertNull(captor.getValue().getAvailability());
    }

    @Test
    @DisplayName("should_throwConflictException_when_emailAlreadyExists")
    void should_throwConflictException_when_emailAlreadyExists() {
        // GIVEN: email already exists in database
        when(userRepository.existsByEmail("raj@example.com")).thenReturn(true);

        // WHEN / THEN
        ConflictException ex = assertThrows(ConflictException.class,
                () -> authService.register(registerRequest));

        assertEquals("Email already registered: raj@example.com", ex.getMessage());
        verify(userRepository, never()).save(any(User.class));
    }

    @Test
    @DisplayName("should_throwUnauthorizedException_when_registeringAsAdmin")
    void should_throwUnauthorizedException_when_registeringAsAdmin() {
        // GIVEN: someone tries to self-register as ADMIN
        registerRequest.setRole(UserRole.ADMIN);
        when(userRepository.existsByEmail("raj@example.com")).thenReturn(false);

        // WHEN / THEN
        UnauthorizedException ex = assertThrows(UnauthorizedException.class,
                () -> authService.register(registerRequest));

        assertEquals("Cannot self-register as ADMIN", ex.getMessage());
        verify(userRepository, never()).save(any(User.class));
    }

    // ── LOGIN TESTS ──

    @Test
    @DisplayName("should_loginSuccessfully_when_validCredentials")
    void should_loginSuccessfully_when_validCredentials() {
        // GIVEN: user exists, password matches
        when(userRepository.findByEmail("raj@example.com")).thenReturn(Optional.of(savedUser));
        when(passwordEncoder.matches("securePass123", "$2a$10$hashedPassword")).thenReturn(true);
        when(jwtService.generateAccessToken(1L, "RIDER")).thenReturn("access-token");
        when(jwtService.generateRefreshToken(1L, "RIDER")).thenReturn("refresh-token");

        // WHEN
        AuthResponse response = authService.login(loginRequest);

        // THEN
        assertNotNull(response);
        assertEquals("access-token", response.getAccessToken());
        assertEquals(UserRole.RIDER, response.getRole());
    }

    @Test
    @DisplayName("should_throwGenericUnauthorized_when_emailNotFound")
    void should_throwGenericUnauthorized_when_emailNotFound() {
        // GIVEN: email doesn't exist in database
        when(userRepository.findByEmail("raj@example.com")).thenReturn(Optional.empty());

        // WHEN / THEN: same 401 + message as a wrong password — no account enumeration
        // (previously a 404 "User not found with email ..." revealed which emails exist)
        UnauthorizedException ex = assertThrows(UnauthorizedException.class,
                () -> authService.login(loginRequest));

        assertEquals("Invalid email or password", ex.getMessage());
        // A dummy BCrypt comparison is still performed, so timing does not reveal the miss either
        verify(passwordEncoder).matches(eq("securePass123"), any());
    }

    @Test
    @DisplayName("should_throwUnauthorizedException_when_wrongPassword")
    void should_throwUnauthorizedException_when_wrongPassword() {
        // GIVEN: user exists but password doesn't match
        when(userRepository.findByEmail("raj@example.com")).thenReturn(Optional.of(savedUser));
        when(passwordEncoder.matches("securePass123", "$2a$10$hashedPassword")).thenReturn(false);

        // WHEN / THEN
        UnauthorizedException ex = assertThrows(UnauthorizedException.class,
                () -> authService.login(loginRequest));

        assertEquals("Invalid email or password", ex.getMessage());
    }

    @Test
    @DisplayName("should_throwUnauthorizedException_when_accountIsSuspended")
    void should_throwUnauthorizedException_when_accountIsSuspended() {
        // GIVEN: user exists but account is suspended (and the password is correct)
        savedUser.setActive(false);
        when(userRepository.findByEmail("raj@example.com")).thenReturn(Optional.of(savedUser));
        when(passwordEncoder.matches("securePass123", "$2a$10$hashedPassword")).thenReturn(true);

        // WHEN / THEN
        UnauthorizedException ex = assertThrows(UnauthorizedException.class,
                () -> authService.login(loginRequest));

        assertEquals("Account is suspended. Contact support.", ex.getMessage());
    }

    @Test
    @DisplayName("should_notRevealSuspension_when_passwordIsWrong")
    void should_notRevealSuspension_when_passwordIsWrong() {
        savedUser.setActive(false);
        when(userRepository.findByEmail("raj@example.com")).thenReturn(Optional.of(savedUser));
        when(passwordEncoder.matches("securePass123", "$2a$10$hashedPassword")).thenReturn(false);

        UnauthorizedException ex = assertThrows(UnauthorizedException.class,
                () -> authService.login(loginRequest));

        assertEquals("Invalid email or password", ex.getMessage());
    }

    // ── EMAIL NORMALIZATION ──

    @Test
    @DisplayName("should_storeLowerCaseEmail_when_registeringWithMixedCase")
    void should_storeLowerCaseEmail_when_registeringWithMixedCase() {
        registerRequest.setEmail("  Raj.Kumar@Example.COM ");
        when(userRepository.existsByEmail("raj.kumar@example.com")).thenReturn(false);
        when(userRepository.save(any(User.class))).thenAnswer(inv -> inv.getArgument(0));

        authService.register(registerRequest);

        ArgumentCaptor<User> captor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(captor.capture());
        assertEquals("raj.kumar@example.com", captor.getValue().getEmail());
    }

    @Test
    @DisplayName("should_lookUpNormalizedEmail_when_loggingInWithMixedCase")
    void should_lookUpNormalizedEmail_when_loggingInWithMixedCase() {
        loginRequest.setEmail("RAJ@Example.com");
        when(userRepository.findByEmail("raj@example.com")).thenReturn(Optional.of(savedUser));
        when(passwordEncoder.matches("securePass123", "$2a$10$hashedPassword")).thenReturn(true);

        assertNotNull(authService.login(loginRequest));
    }

    @Test
    @DisplayName("should_throwConflict_when_concurrentRegistrationHitsUniqueIndex")
    void should_throwConflict_when_concurrentRegistrationHitsUniqueIndex() {
        when(userRepository.existsByEmail("raj@example.com")).thenReturn(false);
        when(userRepository.save(any(User.class)))
                .thenThrow(new DataIntegrityViolationException("uq_users_email_lower"));

        assertThrows(ConflictException.class, () -> authService.register(registerRequest));
    }

    @Test
    @DisplayName("normalizeEmail only trims and lower-cases")
    void normalizeEmail_onlyTrimsAndLowerCases() {
        assertEquals("first.last+tag@sub.example.com",
                AuthService.normalizeEmail("  First.Last+Tag@Sub.Example.COM\t"));
    }

    // ── REFRESH ──

    @Test
    @DisplayName("should_issueNewTokens_when_refreshTokenValidAndUserActive")
    void should_issueNewTokens_when_refreshTokenValidAndUserActive() {
        Claims claims = Jwts.claims().subject("1").build();
        when(jwtService.parseRefreshToken("refresh-token")).thenReturn(Optional.of(claims));
        when(userRepository.findById(1L)).thenReturn(Optional.of(savedUser));
        when(jwtService.generateAccessToken(1L, "RIDER")).thenReturn("new-access");
        when(jwtService.generateRefreshToken(1L, "RIDER")).thenReturn("new-refresh");

        AuthResponse response = authService.refresh("refresh-token");

        assertEquals("new-access", response.getAccessToken());
        assertEquals("new-refresh", response.getRefreshToken());
    }

    @Test
    @DisplayName("should_rejectRefresh_when_tokenIsNotARefreshToken")
    void should_rejectRefresh_when_tokenIsNotARefreshToken() {
        when(jwtService.parseRefreshToken("access-token")).thenReturn(Optional.empty());

        UnauthorizedException ex = assertThrows(UnauthorizedException.class,
                () -> authService.refresh("access-token"));
        assertEquals("Invalid or expired refresh token", ex.getMessage());
        verify(userRepository, never()).findById(anyLong());
    }

    @Test
    @DisplayName("should_rejectRefresh_when_userSuspended")
    void should_rejectRefresh_when_userSuspended() {
        savedUser.setActive(false);
        Claims claims = Jwts.claims().subject("1").build();
        when(jwtService.parseRefreshToken("refresh-token")).thenReturn(Optional.of(claims));
        when(userRepository.findById(1L)).thenReturn(Optional.of(savedUser));

        assertThrows(UnauthorizedException.class, () -> authService.refresh("refresh-token"));
        verify(jwtService, never()).generateAccessToken(anyLong(), anyString());
    }
}
