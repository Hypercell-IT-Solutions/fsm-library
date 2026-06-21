package io.hypercell.fsm.jdbc.dialect;

import io.hypercell.fsm.jdbc.SqlDialect;

/**
 * {@link SqlDialect} for PostgreSQL using {@code ON CONFLICT ... DO UPDATE}.
 * Compatible with PostgreSQL 9.5+.
 */
public class PostgreSqlDialect implements SqlDialect {

    @Override
    public String id() {
        return "postgresql";
    }

    @Override
    public String upsertSql(String tableName) {
        return String.format("""
                INSERT INTO %1$s (
                    execution_id, machine_definition_id, current_state_name, failed_state_name,
                    failed_sub_step_name, last_trigger_event, attempt_number, last_failed_at,
                    scheduled_retry_at, last_error_message, status, captured_at, completed_steps, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON CONFLICT (execution_id) DO UPDATE SET
                    machine_definition_id = EXCLUDED.machine_definition_id,
                    current_state_name    = EXCLUDED.current_state_name,
                    failed_state_name     = EXCLUDED.failed_state_name,
                    failed_sub_step_name  = EXCLUDED.failed_sub_step_name,
                    last_trigger_event    = EXCLUDED.last_trigger_event,
                    attempt_number        = EXCLUDED.attempt_number,
                    last_failed_at        = EXCLUDED.last_failed_at,
                    scheduled_retry_at    = EXCLUDED.scheduled_retry_at,
                    last_error_message    = EXCLUDED.last_error_message,
                    status                = EXCLUDED.status,
                    captured_at           = EXCLUDED.captured_at,
                    completed_steps       = EXCLUDED.completed_steps,
                    version               = %1$s.version + 1
                """, tableName);
    }
}
