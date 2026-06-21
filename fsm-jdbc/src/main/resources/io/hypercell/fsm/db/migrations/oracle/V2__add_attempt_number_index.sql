-- V2: Add composite index on (status, attempt_number) for the recoverFailedExecutions sweep
-- Compatible with Oracle 9i+
-- Note: Oracle does not support CREATE INDEX IF NOT EXISTS

CREATE INDEX idx_fsm_snapshots_status_attempt ON fsm_snapshots (status, attempt_number)
