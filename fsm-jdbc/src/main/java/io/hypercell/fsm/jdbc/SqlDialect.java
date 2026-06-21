package io.hypercell.fsm.jdbc;

/**
 * Database-specific SQL for {@link JdbcSnapshotRepository}.
 *
 * <p>Only the upsert statement varies between databases; every other operation
 * (SELECT, DELETE, listPendingRetries) uses standard SQL shared across all dialects.
 * Schema creation is handled by the file-based {@link io.hypercell.fsm.jdbc.migration.SchemaMigrator},
 * which resolves per-dialect SQL files using the {@link #id()} returned by this interface.
 *
 * <p>Built-in implementations:
 * <ul>
 *   <li>{@link io.hypercell.fsm.jdbc.dialect.PostgreSqlDialect}</li>
 *   <li>{@link io.hypercell.fsm.jdbc.dialect.MySqlDialect}</li>
 *   <li>{@link io.hypercell.fsm.jdbc.dialect.H2Dialect}</li>
 *   <li>{@link io.hypercell.fsm.jdbc.dialect.SqliteDialect}</li>
 *   <li>{@link io.hypercell.fsm.jdbc.dialect.OracleDialect}</li>
 * </ul>
 * Implement this interface to support any other SQL database.
 */
public interface SqlDialect {

    /**
     * Return a stable, lowercase identifier for this dialect.
     *
     * <p>This value is used by {@link io.hypercell.fsm.jdbc.migration.SchemaMigrator} to
     * resolve the correct migration resource folder:
     * {@code io/hypercell/fsm/db/migrations/<id>/}.
     *
     * <p>Built-in values: {@code "postgresql"}, {@code "mysql"}, {@code "h2"},
     * {@code "sqlite"}, {@code "oracle"}.
     *
     * @return the dialect identifier, e.g. {@code "postgresql"}
     */
    String id();

    /**
     * Return the upsert SQL for the given table name.
     *
     * <p>The statement must accept exactly 13 positional parameters in this order:
     * <ol>
     *   <li>execution_id</li>
     *   <li>machine_definition_id</li>
     *   <li>current_state_name</li>
     *   <li>failed_state_name</li>
     *   <li>failed_sub_step_name</li>
     *   <li>last_trigger_event</li>
     *   <li>attempt_number</li>
     *   <li>last_failed_at (ISO-8601 string or null)</li>
     *   <li>scheduled_retry_at (ISO-8601 string or null)</li>
     *   <li>last_error_message</li>
     *   <li>status</li>
     *   <li>captured_at (ISO-8601 string)</li>
     *   <li>completed_steps (serialized text)</li>
     * </ol>
     * The {@code version} column is managed internally — the insert sets it to {@code 1}
     * and the update increments it by {@code 1} without requiring a caller-supplied value.
     *
     * @param tableName the table name, e.g. {@code "fsm_snapshots"}
     * @return the fully formed upsert SQL string
     */
    String upsertSql(String tableName);

    /**
     * Return the row-limiting clause appended to the end of a {@code SELECT} (after
     * {@code ORDER BY}) to cap the number of returned rows.
     *
     * <p>The default {@code LIMIT n} works on PostgreSQL, MySQL, H2 and SQLite. Oracle does
     * not support {@code LIMIT} and overrides this with {@code FETCH FIRST n ROWS ONLY}
     * (Oracle 12c+).
     *
     * @param limit the maximum number of rows
     * @return the limiting clause, e.g. {@code "LIMIT 100"}
     */
    default String limitClause(int limit) {
        return "LIMIT " + limit;
    }
}
