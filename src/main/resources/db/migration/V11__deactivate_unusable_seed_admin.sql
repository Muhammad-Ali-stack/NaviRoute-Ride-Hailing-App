-- =============================================================
-- V11: Deactivate the administrator seeded by V7 if its password was never changed.
-- The V7 password hash does not match the documented password, so this account
-- cannot be used, yet it remains a known admin email with a published hash.
-- Only the untouched seed row (same email, role and original hash) is affected;
-- an admin whose password was changed after V7 is left active.
--
-- To use this account, set NAVIROUTE_ADMIN_EMAIL=admin@ridesharing.com and
-- NAVIROUTE_ADMIN_PASSWORD; AdminBootstrap then sets the password and reactivates it.
-- =============================================================

UPDATE users
SET active = FALSE
WHERE email = 'admin@ridesharing.com'
  AND role = 'ADMIN'
  AND password = '$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy';
