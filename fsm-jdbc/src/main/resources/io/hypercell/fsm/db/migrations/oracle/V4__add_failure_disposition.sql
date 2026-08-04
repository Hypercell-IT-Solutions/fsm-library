-- V4: Add failure classification columns and the sweep index
-- Compatible with Oracle 9i+
-- Oracle uses VARCHAR2 and supports neither ADD COLUMN IF NOT EXISTS nor
-- CREATE INDEX IF NOT EXISTS; it also omits the COLUMN keyword and groups additions in ADD (...).
-- The migrator only runs a version not already recorded in fsm_schema_history, so these run once.
--
-- failure_disposition records how a failure should be handled (FailureDisposition enum).
-- The DEFAULT backfills every existing row as RETRY, which is exactly how the library
-- behaved before dispositions existed, so no data migration is needed.
-- last_error_type completes the durable error record alongside last_error_message.

ALTER TABLE fsm_snapshots
    ADD (failure_disposition VARCHAR2(30) DEFAULT 'RETRY' NOT NULL);

ALTER TABLE fsm_snapshots
    ADD (last_error_type VARCHAR2(255));

-- Covers the recoverFailedExecutions sweep:
--   WHERE status = 'FAILED' AND failure_disposition = 'RETRY' AND attempt_number < ?
-- The V2 index on (status, attempt_number) is kept; it still serves listPendingRetries.
CREATE INDEX idx_fsm_snapshots_sweep
    ON fsm_snapshots (status, failure_disposition, attempt_number)
