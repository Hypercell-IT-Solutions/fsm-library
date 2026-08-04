-- V4: Add failure classification columns and the sweep index
-- Compatible with SQLite 3.24.0+
-- Note: SQLite has no ADD COLUMN IF NOT EXISTS, but the migrator only runs a version that is
--       not already recorded in fsm_schema_history, so the ALTERs run exactly once. A NOT NULL
--       column can only be added when it has a literal default, which 'RETRY' satisfies.
--
-- failure_disposition records how a failure should be handled (FailureDisposition enum).
-- The NOT NULL DEFAULT backfills every existing row as RETRY, which is exactly how the
-- library behaved before dispositions existed, so no data migration is needed.
-- last_error_type completes the durable error record alongside last_error_message.

ALTER TABLE fsm_snapshots
    ADD COLUMN failure_disposition TEXT NOT NULL DEFAULT 'RETRY';

ALTER TABLE fsm_snapshots
    ADD COLUMN last_error_type TEXT;

-- Covers the recoverFailedExecutions sweep:
--   WHERE status = 'FAILED' AND failure_disposition = 'RETRY' AND attempt_number < ?
-- The V2 index on (status, attempt_number) is kept; it still serves listPendingRetries.
CREATE INDEX IF NOT EXISTS idx_fsm_snapshots_sweep
    ON fsm_snapshots (status, failure_disposition, attempt_number)
