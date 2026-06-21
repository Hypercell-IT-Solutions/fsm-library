-- V2: Add composite index on (status, attempt_number) for the recoverFailedExecutions sweep
-- Compatible with MySQL 5.7+ and MariaDB 10.3+
-- Note: MySQL does not support CREATE INDEX IF NOT EXISTS; run separately and ignore duplicate errors

CREATE INDEX idx_fsm_snapshots_status_attempt ON fsm_snapshots (status, attempt_number)
