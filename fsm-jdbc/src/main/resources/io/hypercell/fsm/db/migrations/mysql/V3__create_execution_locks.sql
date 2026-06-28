-- V3: Create the fsm_execution_locks table for distributed per-execution locking
-- Compatible with MySQL 5.7+ and MariaDB 10.3+

CREATE TABLE IF NOT EXISTS fsm_execution_locks
(
    execution_id VARCHAR(255) NOT NULL,
    -- NULL = unlocked; non-NULL = locked (host@pid#thread identifier)
    locked_by    VARCHAR(255),
    -- ISO-8601 timestamp of when the lock was acquired; used for stale-lock TTL check
    locked_at    VARCHAR(50),
    CONSTRAINT pk_fsm_execution_locks PRIMARY KEY (execution_id)
)
