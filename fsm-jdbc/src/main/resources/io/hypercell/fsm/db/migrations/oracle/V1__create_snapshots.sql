-- V1: Create the fsm_snapshots table and its status index
-- Compatible with Oracle 9i+
-- Oracle uses VARCHAR2/CLOB/NUMBER instead of VARCHAR/TEXT/BIGINT.
-- Oracle does not support CREATE TABLE IF NOT EXISTS or CREATE INDEX IF NOT EXISTS.

CREATE TABLE fsm_snapshots
(
    execution_id          VARCHAR2(255)        NOT NULL,
    machine_definition_id VARCHAR2(255)        NOT NULL,
    current_state_name    VARCHAR2(255),
    failed_state_name     VARCHAR2(255),
    failed_sub_step_name  VARCHAR2(255),
    last_trigger_event    VARCHAR2(255),
    attempt_number        NUMBER(10) DEFAULT 1 NOT NULL,
    -- Stored as ISO-8601 strings to avoid timezone handling differences across JDBC drivers
    last_failed_at        VARCHAR2(50),
    scheduled_retry_at    VARCHAR2(50),
    captured_at           VARCHAR2(50)         NOT NULL,
    last_error_message    CLOB,
    -- SnapshotStatus enum value: RUNNING, FAILED, RETRY_SCHEDULED, COMPLETED
    status                VARCHAR2(50)         NOT NULL,
    -- JSON encoding of completedSubStepResults
    completed_steps       CLOB,
    -- Incremented on every save; basis for optimistic locking
    version               NUMBER(19) DEFAULT 1 NOT NULL,
    CONSTRAINT pk_fsm_snapshots PRIMARY KEY (execution_id)
);

-- Index on status speeds up listPendingRetries() queries
CREATE INDEX idx_fsm_snapshots_status ON fsm_snapshots (status)
