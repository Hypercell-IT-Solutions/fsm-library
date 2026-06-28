-- V3: Create the fsm_execution_locks table for distributed per-execution locking
-- Compatible with SQLite 3.24.0+ (2018-06-04)

CREATE TABLE IF NOT EXISTS fsm_execution_locks
(
    execution_id TEXT NOT NULL,
    -- NULL = unlocked; non-NULL = locked (host@pid#thread identifier)
    locked_by    TEXT,
    -- ISO-8601 timestamp of when the lock was acquired; used for stale-lock TTL check
    locked_at    TEXT,
    PRIMARY KEY (execution_id)
)
