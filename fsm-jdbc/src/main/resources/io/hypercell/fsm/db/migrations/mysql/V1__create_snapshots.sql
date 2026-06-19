-- V1: Create the fsm_snapshots table and its status index
-- Compatible with MySQL 5.7+ and MariaDB 10.3+
-- Note: MySQL < 8.0.1 does not support CREATE INDEX IF NOT EXISTS; here we use a plain CREATE INDEX.

CREATE TABLE IF NOT EXISTS fsm_snapshots
(
    execution_id          VARCHAR(255) NOT NULL,
    machine_definition_id VARCHAR(255) NOT NULL,
    current_state_name    VARCHAR(255),
    failed_state_name     VARCHAR(255),
    failed_sub_step_name  VARCHAR(255),
    last_trigger_event    VARCHAR(255),
    attempt_number        INT          NOT NULL DEFAULT 1,
    -- Stored as ISO-8601 strings to avoid timezone handling differences across JDBC drivers
    last_failed_at        VARCHAR(50),
    scheduled_retry_at    VARCHAR(50),
    captured_at           VARCHAR(50)  NOT NULL,
    last_error_message    TEXT,
    -- SnapshotStatus enum value: RUNNING, FAILED, RETRY_SCHEDULED, COMPLETED
    status                VARCHAR(50)  NOT NULL,
    -- JSON encoding of completedSubStepResults
    completed_steps       TEXT,
    -- Incremented on every save; basis for optimistic locking
    version               BIGINT       NOT NULL DEFAULT 1,
    CONSTRAINT pk_fsm_snapshots PRIMARY KEY (execution_id)
);

-- MySQL does not support CREATE INDEX IF NOT EXISTS; run separately and ignore duplicate errors
CREATE INDEX idx_fsm_snapshots_status ON fsm_snapshots (status)
