-- =============================================================
-- V10: Initialize approval/availability state for existing drivers.
-- Registration previously left approval_status NULL for new drivers, so they
-- never appeared in the admin "pending drivers" list. Registration now sets
-- PENDING/OFFLINE; this backfills drivers created before the fix.
-- Only rows still NULL are touched; reviewed drivers are unchanged.
-- =============================================================

UPDATE users
SET approval_status = 'PENDING'
WHERE role = 'DRIVER' AND approval_status IS NULL;

UPDATE users
SET availability = 'OFFLINE'
WHERE role = 'DRIVER' AND availability IS NULL;
