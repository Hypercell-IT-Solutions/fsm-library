-- V4: Add failure classification columns and the sweep index
-- Compatible with H2 2.x
--
-- failure_disposition records how a failure should be handled (FailureDisposition enum).
-- The NOT NULL DEFAULT backfills every existing row as RETRY, which is exactly how the
-- library behaved before dispositions existed, so no data migration is needed.
-- last_error_type completes the durable error record alongside last_error_message.

ALTER TABLE fsm_snapshots
    ADD COLUMN IF NOT EXISTS failure_disposition VARCHAR (30) DEFAULT 'RETRY' NOT NULL;

ALTER TABLE fsm_snapshots
    ADD COLUMN IF NOT EXISTS last_error_type VARCHAR (255);

-- Covers the recoverFailedExecutions sweep:
--   WHERE status = 'FAILED' AND failure_disposition = 'RETRY' AND attempt_number < ?
-- The V2 index on (status, attempt_number) is kept; it still serves listPendingRetries.
CREATE INDEX IF NOT EXISTS idx_fsm_snapshots_sweep
    ON fsm_snapshots (status, failure_disposition, attempt_number)
