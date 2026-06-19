package io.hypercell.fsm.jdbc.migration;

/**
 * Controls how {@link SchemaMigrator} behaves at startup.
 *
 * <ul>
 *   <li>{@link #UPDATE} — the default. Applies all pending migrations automatically,
 *       matching today's ad-hoc {@code initSchema()} behaviour. Safe for development and
 *       for environments where the service account has DDL privileges.</li>
 *   <li>{@link #VALIDATE} — no DDL is executed. The migrator compares the history table
 *       against the bundled registry and fails fast if any version is missing, logging the
 *       exact pending SQL so operators can run it out-of-band. Use in production where the
 *       application user must not hold DDL rights.</li>
 *   <li>{@link #OFF} — schema management is completely disabled. The repository assumes
 *       the schema is present and correct. No tables are created or checked.</li>
 * </ul>
 */
public enum MigrationMode {

    /**
     * Apply pending migrations automatically (default).
     */
    UPDATE,

    /**
     * Validate that the DB is fully up to date; fail fast if not.
     * Logs pending SQL for out-of-band execution.
     */
    VALIDATE,

    /**
     * Skip all schema management. The caller is responsible for the schema.
     */
    OFF
}
