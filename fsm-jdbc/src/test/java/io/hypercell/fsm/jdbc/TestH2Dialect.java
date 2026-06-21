package io.hypercell.fsm.jdbc;

/**
 * H2-compatible MERGE-based upsert dialect for tests.
 * H2 2.x in PostgreSQL mode has issues with text-block SQL containing
 * newlines before ON CONFLICT. MERGE INTO works in all H2 modes.
 */
class TestH2Dialect implements SqlDialect {

    @Override
    public String id() {
        return "h2";
    }

    @Override
    public String upsertSql(String tableName) {
        return "MERGE INTO " + tableName
                + " (execution_id, machine_definition_id, current_state_name, failed_state_name,"
                + " failed_sub_step_name, last_trigger_event, attempt_number, last_failed_at,"
                + " scheduled_retry_at, last_error_message, status, captured_at, completed_steps, version)"
                + " KEY (execution_id)"
                + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)";
    }
}
