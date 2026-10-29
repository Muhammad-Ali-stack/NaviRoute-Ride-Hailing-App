-- =============================================================
-- V13: Email addresses are unique regardless of letter case.
--
-- V1 made users.email unique, but case-sensitively, so "A@x.com" and "a@x.com" could be
-- two accounts. The application now looks emails up by LOWER(email) and stores new
-- addresses in lower case; this functional unique index enforces it in the database
-- (including concurrent registrations) and serves those lookups.
--
-- Existing rows are NOT rewritten (stored addresses keep their original case; lookups
-- are case-insensitive). Accounts that collide by case are NOT merged automatically.
-- If any exist, the migration stops with an explicit error; decide per pair which
-- account to keep (e.g. change one address) and re-run. Detection query:
--   SELECT LOWER(email), array_agg(id ORDER BY id) FROM users
--     GROUP BY LOWER(email) HAVING COUNT(*) > 1;
-- =============================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM users GROUP BY LOWER(email) HAVING COUNT(*) > 1) THEN
        RAISE EXCEPTION 'V13: some accounts have emails that differ only by letter case; resolve them before migrating (see V13 header for the query)';
    END IF;
END $$;

CREATE UNIQUE INDEX uq_users_email_lower ON users (LOWER(email));
