-- =============================================================
-- V12: At most one active ride per rider and per driver, enforced by the database.
--
-- The application checks this before inserting/assigning, but two concurrent requests
-- could both pass that check. These partial unique indexes make the rule hold under
-- concurrency:
--   - a rider may have only one ride in REQUESTED / ACCEPTED / IN_PROGRESS
--   - a driver may be assigned to only one ride in ACCEPTED / IN_PROGRESS
--
-- Existing data that already violates the rule is NOT modified automatically. The
-- migration stops with an explicit error instead; resolve the listed rides by hand
-- (e.g. cancel stale duplicates) and re-run. Detection queries:
--   SELECT rider_id, COUNT(*) FROM rides
--     WHERE status IN ('REQUESTED','ACCEPTED','IN_PROGRESS') GROUP BY rider_id HAVING COUNT(*) > 1;
--   SELECT driver_id, COUNT(*) FROM rides
--     WHERE status IN ('ACCEPTED','IN_PROGRESS') AND driver_id IS NOT NULL
--     GROUP BY driver_id HAVING COUNT(*) > 1;
-- =============================================================

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM rides
               WHERE status IN ('REQUESTED', 'ACCEPTED', 'IN_PROGRESS')
               GROUP BY rider_id HAVING COUNT(*) > 1) THEN
        RAISE EXCEPTION 'V12: some riders have more than one active ride; resolve them before migrating (see V12 header for the query)';
    END IF;
    IF EXISTS (SELECT 1 FROM rides
               WHERE status IN ('ACCEPTED', 'IN_PROGRESS') AND driver_id IS NOT NULL
               GROUP BY driver_id HAVING COUNT(*) > 1) THEN
        RAISE EXCEPTION 'V12: some drivers are assigned to more than one active ride; resolve them before migrating (see V12 header for the query)';
    END IF;
END $$;

CREATE UNIQUE INDEX uq_rides_rider_active
    ON rides (rider_id)
    WHERE status IN ('REQUESTED', 'ACCEPTED', 'IN_PROGRESS');

CREATE UNIQUE INDEX uq_rides_driver_active
    ON rides (driver_id)
    WHERE status IN ('ACCEPTED', 'IN_PROGRESS') AND driver_id IS NOT NULL;
