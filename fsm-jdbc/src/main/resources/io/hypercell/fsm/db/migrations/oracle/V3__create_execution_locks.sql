-- V3: Create the fsm_execution_locks table for distributed per-execution locking
-- Compatible with Oracle 9i+
-- Oracle does not support CREATE TABLE IF NOT EXISTS or VARCHAR/TEXT.

CREATE TABLE fsm_execution_locks
(
    execution_id VARCHAR2(255) NOT NULL,
    -- NULL = unlocked; non-NULL = locked (host@pid#thread identifier)
    locked_by    VARCHAR2(255),
    -- ISO-8601 timestamp of when the lock was acquired; used for stale-lock TTL check
    locked_at    VARCHAR2(50),
    CONSTRAINT pk_fsm_execution_locks PRIMARY KEY (execution_id)
)
