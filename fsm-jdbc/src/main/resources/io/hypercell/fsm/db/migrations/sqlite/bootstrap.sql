-- FSM schema migration bootstrap for SQLite
-- Creates tracking tables and seeds the single lock row.
-- Safe to re-run: CREATE TABLE IF NOT EXISTS and INSERT OR IGNORE are used.

CREATE TABLE IF NOT EXISTS fsm_schema_history
(
    version          INTEGER NOT NULL,
    description      TEXT    NOT NULL,
    checksum         TEXT    NOT NULL,
    applied_at       TEXT    NOT NULL,
    execution_millis INTEGER NOT NULL,
    success          INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (version)
);

CREATE TABLE IF NOT EXISTS fsm_schema_lock
(
    id        INTEGER NOT NULL,
    locked    INTEGER NOT NULL DEFAULT 0,
    locked_by TEXT,
    locked_at TEXT,
    PRIMARY KEY (id)
);

-- Seed the single lock row (id=1); ignore if it already exists
INSERT OR IGNORE INTO fsm_schema_lock (id, locked, locked_by, locked_at)
VALUES (1, 0, NULL, NULL)
