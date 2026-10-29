package com.ridesharing.auth.config;

import com.ridesharing.auth.model.User;
import com.ridesharing.auth.repository.UserRepository;
import com.ridesharing.shared.enums.UserRole;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class AdminBootstrapTest {

    private static final String EMAIL = "ops@naviroute.test";
    private static final String PASSWORD = "a-long-test-password";

    @Mock
    private UserRepository userRepository;

    @Mock
    private PasswordEncoder passwordEncoder;

    private AdminBootstrap bootstrap(String email, String password) {
        return new AdminBootstrap(userRepository, passwordEncoder, email, password);
    }

    @Test
    @DisplayName("Does nothing when not configured")
    void noConfiguration() {
        bootstrap("", "").run(null);

        verifyNoInteractions(userRepository, passwordEncoder);
    }

    @Test
    @DisplayName("Creates an active ADMIN with an encoded password when the account does not exist")
    void createsAdmin() {
        when(userRepository.findByEmail(EMAIL)).thenReturn(Optional.empty());
        when(passwordEncoder.encode(PASSWORD)).thenReturn("encoded");

        bootstrap(EMAIL, PASSWORD).run(null);

        ArgumentCaptor<User> captor = ArgumentCaptor.forClass(User.class);
        verify(userRepository).save(captor.capture());
        User admin = captor.getValue();
        assertEquals(EMAIL, admin.getEmail());
        assertEquals(UserRole.ADMIN, admin.getRole());
        assertEquals("encoded", admin.getPassword());
        assertTrue(admin.isActive());
    }

    @Test
    @DisplayName("Leaves an existing administrator's credentials unchanged on restart")
    void doesNotResetExistingAdmin() {
        User existing = User.builder().email(EMAIL).role(UserRole.ADMIN)
                .password("$2a$10$someOtherHash").active(true).build();
        when(userRepository.findByEmail(EMAIL)).thenReturn(Optional.of(existing));

        bootstrap(EMAIL, PASSWORD).run(null);

        verify(userRepository, never()).save(any());
        verify(passwordEncoder, never()).encode(anyString());
        assertEquals("$2a$10$someOtherHash", existing.getPassword());
    }

    @Test
    @DisplayName("Repairs the unusable V7 seed admin once (new password, reactivated)")
    void repairsV7SeedAdmin() {
        User seed = User.builder().email(EMAIL).role(UserRole.ADMIN)
                .password(AdminBootstrap.V7_SEED_PASSWORD_HASH).active(false).build();
        when(userRepository.findByEmail(EMAIL)).thenReturn(Optional.of(seed));
        when(passwordEncoder.encode(PASSWORD)).thenReturn("encoded");

        bootstrap(EMAIL, PASSWORD).run(null);

        assertEquals("encoded", seed.getPassword());
        assertTrue(seed.isActive());
        verify(userRepository).save(seed);
    }

    @Test
    @DisplayName("Refuses to elevate an existing non-admin account")
    void refusesNonAdmin() {
        User rider = User.builder().email(EMAIL).role(UserRole.RIDER).password("x").active(true).build();
        when(userRepository.findByEmail(EMAIL)).thenReturn(Optional.of(rider));

        assertThrows(IllegalStateException.class, () -> bootstrap(EMAIL, PASSWORD).run(null));
        assertEquals(UserRole.RIDER, rider.getRole());
        verify(userRepository, never()).save(any());
    }

    @Test
    @DisplayName("Fails fast when only one of email/password is set")
    void partialConfiguration() {
        assertThrows(IllegalStateException.class, () -> bootstrap(EMAIL, "").run(null));
        assertThrows(IllegalStateException.class, () -> bootstrap("", PASSWORD).run(null));
        verifyNoInteractions(userRepository);
    }

    @Test
    @DisplayName("Rejects an invalid email or a short password without echoing the password")
    void validatesCredentials() {
        IllegalStateException shortPw = assertThrows(IllegalStateException.class,
                () -> bootstrap(EMAIL, "short-pw").run(null));
        assertFalse(shortPw.getMessage().contains("short-pw"));

        assertThrows(IllegalStateException.class, () -> bootstrap("not-an-email", PASSWORD).run(null));
        verifyNoInteractions(userRepository);
    }
}
