-- FSM schema migration bootstrap for Oracle Database
-- Creates tracking tables and seeds the single lock row.
-- Oracle does not support CREATE TABLE IF NOT EXISTS or INSERT IGNORE;
-- "already exists" errors (ORA-00955, ORA-00001) are caught and ignored by SchemaMigrator.

CREATE TABLE fsm_schema_history
(
    version          NUMBER(10)          NOT NULL,
    description      VARCHAR2(255)       NOT NULL,
    checksum         VARCHAR2(64)        NOT NULL,
    applied_at       VARCHAR2(50)        NOT NULL,
    execution_millis NUMBER(19)          NOT NULL,
    success          NUMBER(1) DEFAULT 1 NOT NULL,
    CONSTRAINT pk_fsm_schema_history PRIMARY KEY (version)
);

CREATE TABLE fsm_schema_lock
(
    id        NUMBER(10)          NOT NULL,
    locked    NUMBER(1) DEFAULT 0 NOT NULL,
    locked_by VARCHAR2(255),
    locked_at VARCHAR2(50),
    CONSTRAINT pk_fsm_schema_lock PRIMARY KEY (id)
);

-- Seed the single lock row (id=1)
INSERT INTO fsm_schema_lock (id, locked, locked_by, locked_at)
VALUES (1, 0, NULL, NULL)
