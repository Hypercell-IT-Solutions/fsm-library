-- V2: Add composite index on (status, attempt_number) for the recoverFailedExecutions sweep
-- Compatible with PostgreSQL 9.5+

CREATE INDEX IF NOT EXISTS idx_fsm_snapshots_status_attempt ON fsm_snapshots (status, attempt_number)
