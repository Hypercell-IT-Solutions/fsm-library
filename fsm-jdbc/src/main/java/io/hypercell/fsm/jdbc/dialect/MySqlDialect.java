package io.hypercell.fsm.jdbc.dialect;

import io.hypercell.fsm.jdbc.SqlDialect;

/**
 * {@link SqlDialect} for MySQL and MariaDB using {@code ON DUPLICATE KEY UPDATE}.
 * Compatible with MySQL 5.7+ and MariaDB 10.3+.
 */
public class MySqlDialect implements SqlDialect {

    @Override
    public String id() {
        return "mysql";
    }

    @Override
    public String upsertSql(String tableName) {
        return String.format("""
                INSERT INTO %s (
                    execution_id, machine_definition_id, current_state_name, failed_state_name,
                    failed_sub_step_name, last_trigger_event, attempt_number, last_failed_at,
                    scheduled_retry_at, last_error_message, last_error_type, failure_disposition,
                    status, captured_at, completed_steps, version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                ON DUPLICATE KEY UPDATE
                    machine_definition_id = VALUES(machine_definition_id),
                    current_state_name    = VALUES(current_state_name),
                    failed_state_name     = VALUES(failed_state_name),
                    failed_sub_step_name  = VALUES(failed_sub_step_name),
                    last_trigger_event    = VALUES(last_trigger_event),
                    attempt_number        = VALUES(attempt_number),
                    last_failed_at        = VALUES(last_failed_at),
                    scheduled_retry_at    = VALUES(scheduled_retry_at),
                    last_error_message    = VALUES(last_error_message),
                    last_error_type       = VALUES(last_error_type),
                    failure_disposition   = VALUES(failure_disposition),
                    status                = VALUES(status),
                    captured_at           = VALUES(captured_at),
                    completed_steps       = VALUES(completed_steps),
                    version               = version + 1
                """, tableName);
    }
}
