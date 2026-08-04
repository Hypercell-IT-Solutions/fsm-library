package io.hypercell.fsm.jdbc.dialect;

import io.hypercell.fsm.jdbc.SqlDialect;

/**
 * {@link SqlDialect} for Oracle Database using {@code MERGE INTO ... USING (SELECT ... FROM DUAL)}.
 * Compatible with Oracle 9i+.
 *
 * <p>Oracle-specific schema differences (VARCHAR2, CLOB, NUMBER) are handled in the
 * per-dialect migration files under
 * {@code io/hypercell/fsm/db/migrations/oracle/} rather than in this class.
 */
public class OracleDialect implements SqlDialect {

    @Override
    public String id() {
        return "oracle";
    }

    @Override
    public String upsertSql(String tableName) {
        return String.format("""
                MERGE INTO %1$s t
                USING (
                    SELECT
                        ? AS execution_id,
                        ? AS machine_definition_id,
                        ? AS current_state_name,
                        ? AS failed_state_name,
                        ? AS failed_sub_step_name,
                        ? AS last_trigger_event,
                        ? AS attempt_number,
                        ? AS last_failed_at,
                        ? AS scheduled_retry_at,
                        ? AS last_error_message,
                        ? AS last_error_type,
                        ? AS failure_disposition,
                        ? AS status,
                        ? AS captured_at,
                        ? AS completed_steps
                    FROM DUAL
                ) s ON (t.execution_id = s.execution_id)
                WHEN MATCHED THEN UPDATE SET
                    t.machine_definition_id = s.machine_definition_id,
                    t.current_state_name    = s.current_state_name,
                    t.failed_state_name     = s.failed_state_name,
                    t.failed_sub_step_name  = s.failed_sub_step_name,
                    t.last_trigger_event    = s.last_trigger_event,
                    t.attempt_number        = s.attempt_number,
                    t.last_failed_at        = s.last_failed_at,
                    t.scheduled_retry_at    = s.scheduled_retry_at,
                    t.last_error_message    = s.last_error_message,
                    t.last_error_type       = s.last_error_type,
                    t.failure_disposition   = s.failure_disposition,
                    t.status                = s.status,
                    t.captured_at           = s.captured_at,
                    t.completed_steps       = s.completed_steps,
                    t.version               = t.version + 1
                WHEN NOT MATCHED THEN INSERT (
                    execution_id, machine_definition_id, current_state_name, failed_state_name,
                    failed_sub_step_name, last_trigger_event, attempt_number, last_failed_at,
                    scheduled_retry_at, last_error_message, last_error_type, failure_disposition,
                    status, captured_at, completed_steps, version
                ) VALUES (
                    s.execution_id, s.machine_definition_id, s.current_state_name, s.failed_state_name,
                    s.failed_sub_step_name, s.last_trigger_event, s.attempt_number, s.last_failed_at,
                    s.scheduled_retry_at, s.last_error_message, s.last_error_type, s.failure_disposition,
                    s.status, s.captured_at, s.completed_steps, 1
                )
                """, tableName);
    }

    /**
     * Oracle does not support {@code LIMIT}; use {@code FETCH FIRST n ROWS ONLY} (Oracle 12c+).
     */
    @Override
    public String limitClause(int limit) {
        return "FETCH FIRST " + limit + " ROWS ONLY";
    }
}
