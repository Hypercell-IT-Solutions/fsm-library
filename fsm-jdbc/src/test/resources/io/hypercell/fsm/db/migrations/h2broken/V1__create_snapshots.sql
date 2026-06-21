-- V1: Create the fsm_snapshots table — intentionally broken for transaction tests.
-- The first statement is valid; the second is deliberately invalid SQL.
-- Used to verify that a mid-migration failure rolls back the history insert.

CREATE TABLE IF NOT EXISTS fsm_snapshots
(
    execution_id          VARCHAR(255) NOT NULL,
    machine_definition_id VARCHAR(255) NOT NULL,
    status                VARCHAR(50)  NOT NULL,
    captured_at           VARCHAR(50)  NOT NULL,
    version               BIGINT       NOT NULL DEFAULT 1,
    CONSTRAINT pk_fsm_snapshots PRIMARY KEY (execution_id)
);

THIS IS NOT VALID SQL AND SHOULD CAUSE A PARSE ERROR
