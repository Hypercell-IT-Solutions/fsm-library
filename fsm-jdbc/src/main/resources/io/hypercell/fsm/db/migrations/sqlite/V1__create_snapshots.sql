-- V1: Create the fsm_snapshots table and its status index
-- Compatible with SQLite 3.24.0+ (2018-06-04)

CREATE TABLE IF NOT EXISTS fsm_snapshots
(
    execution_id          TEXT    NOT NULL,
    machine_definition_id TEXT    NOT NULL,
    current_state_name    TEXT,
    failed_state_name     TEXT,
    failed_sub_step_name  TEXT,
    last_trigger_event    TEXT,
    attempt_number        INTEGER NOT NULL DEFAULT 1,
    -- Stored as ISO-8601 strings to avoid timezone handling differences
    last_failed_at        TEXT,
    scheduled_retry_at    TEXT,
    captured_at           TEXT    NOT NULL,
    last_error_message    TEXT,
    -- SnapshotStatus enum value: RUNNING, FAILED, RETRY_SCHEDULED, COMPLETED
    status                TEXT    NOT NULL,
    -- JSON encoding of completedSubStepResults
    completed_steps       TEXT,
    -- Incremented on every save; basis for optimistic locking
    version               INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (execution_id)
);

-- Index on status speeds up listPendingRetries() queries
CREATE INDEX IF NOT EXISTS idx_fsm_snapshots_status ON fsm_snapshots (status)
