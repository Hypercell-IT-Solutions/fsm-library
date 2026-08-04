package io.hypercell.fsm.jdbc.dialect;

import io.hypercell.fsm.jdbc.SqlDialect;

/**
 * {@link SqlDialect} for SQLite using {@code ON CONFLICT(...) DO UPDATE}.
 * Compatible with SQLite 3.24.0+ (released 2018-06-04).
 */
public class SqliteDialect implements SqlDialect {

    @Override
    public String id() {
        return "sqlite";
    }

    @Override
    public String upsertSql(String tableName) {
        return String.format("""
                INSERT INTO %1$s (
                    execution_id, machine_definition_id, current_state_name, failed_state_name,
                    failed_sub_step_name, last_trigger_event, attempt_number, last_failed_at,
                    scheduled_retry_at, last_error_message, last_error_type, failure_disposition,
                    status, captured_at, completed_steps, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT(execution_id) DO UPDATE SET
                    machine_definition_id = excluded.machine_definition_id,
                    current_state_name    = excluded.current_state_name,
                    failed_state_name     = excluded.failed_state_name,
                    failed_sub_step_name  = excluded.failed_sub_step_name,
                    last_trigger_event    = excluded.last_trigger_event,
                    attempt_number        = excluded.attempt_number,
                    last_failed_at        = excluded.last_failed_at,
                    scheduled_retry_at    = excluded.scheduled_retry_at,
                    last_error_message    = excluded.last_error_message,
                    last_error_type       = excluded.last_error_type,
                    failure_disposition   = excluded.failure_disposition,
                    status                = excluded.status,
                    captured_at           = excluded.captured_at,
                    completed_steps       = excluded.completed_steps,
                    version               = %1$s.version + 1
                """, tableName);
    }
}
