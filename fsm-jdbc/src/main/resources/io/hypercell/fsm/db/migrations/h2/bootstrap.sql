-- FSM schema migration bootstrap for H2
-- Creates tracking tables and seeds the single lock row.
-- Safe to re-run: CREATE TABLE IF NOT EXISTS and conditional INSERT are used.

CREATE TABLE IF NOT EXISTS fsm_schema_history
(
    version          INT          NOT NULL,
    description      VARCHAR(255) NOT NULL,
    checksum         VARCHAR(64)  NOT NULL,
    applied_at       VARCHAR(50)  NOT NULL,
    execution_millis BIGINT       NOT NULL,
    success          INT          NOT NULL DEFAULT 1,
    CONSTRAINT pk_fsm_schema_history PRIMARY KEY (version)
);

CREATE TABLE IF NOT EXISTS fsm_schema_lock
(
    id        INT NOT NULL,
    locked    INT NOT NULL DEFAULT 0,
    locked_by VARCHAR(255),
    locked_at VARCHAR(50),
    CONSTRAINT pk_fsm_schema_lock PRIMARY KEY (id)
);

-- Seed the single lock row (id=1) only if it does not already exist
INSERT INTO fsm_schema_lock (id, locked, locked_by, locked_at)
SELECT 1, 0, NULL, NULL
WHERE NOT EXISTS (SELECT 1 FROM fsm_schema_lock WHERE id = 1)
