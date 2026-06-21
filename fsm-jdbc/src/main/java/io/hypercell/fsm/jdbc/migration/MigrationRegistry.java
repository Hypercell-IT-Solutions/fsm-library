package io.hypercell.fsm.jdbc.migration;

import java.util.List;

/**
 * Ordered manifest of all schema migration versions bundled with this library.
 *
 * <p>This registry is the <em>ordering index only</em>: it lists every version number and its
 * description in ascending order. The actual SQL lives in per-dialect files on the classpath
 * (see {@link Migration#resourcePath(String)}). Keeping ordering here — rather than scanning
 * the classpath — ensures deterministic ordering inside JARs where directory listing is
 * unreliable across classloaders.
 *
 * <p>To add a new version: append a new {@link Migration} entry here AND add the corresponding
 * SQL file under every dialect folder in
 * {@code fsm-jdbc/src/main/resources/io/hypercell/fsm/db/migrations/<dialect>/}.
 *
 * <p>Current versions:
 * <ol>
 *   <li>V1 — {@code create_snapshots}: the initial {@code fsm_snapshots} table and its status index.</li>
 *   <li>V2 — {@code add_attempt_number_index}: composite index on {@code (status, attempt_number)}
 *       to accelerate the {@code recoverFailedExecutions} sweep query.</li>
 * </ol>
 */
public final class MigrationRegistry {

    /**
     * V1 — creates the {@code fsm_snapshots} table and its status index.
     * This is the baseline schema introduced in library version 1.0.0-RC1.
     */
    public static final Migration V1 = new Migration(1, "create_snapshots");

    /**
     * V2 — adds a composite index on {@code (status, attempt_number)} to accelerate
     * the {@link io.hypercell.fsm.manager.StateMachineManager#recoverFailedExecutions(int)}
     * sweep query ({@code WHERE status = 'FAILED' AND attempt_number < ?}).
     * Introduced in library version 1.0.0-RC2.
     */
    public static final Migration V2 = new Migration(2, "add_attempt_number_index");

    /**
     * The complete ordered list of all registered migrations.
     * {@link SchemaMigrator} iterates this list in order to determine which versions to apply.
     */
    public static final List<Migration> ALL = List.of(V1, V2);

    private MigrationRegistry() {
    }
}
