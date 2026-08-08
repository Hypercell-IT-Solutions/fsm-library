package io.hypercell.fsm.jdbc.migration;

import io.hypercell.fsm.jdbc.SqlDialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * Liquibase-style, dependency-free schema migration runner for FSM's JDBC module.
 *
 * <h2>Overview</h2>
 * <p>On each call to {@link #migrate()} (behaviour depends on the configured
 * {@link MigrationMode}), the migrator:
 * <ol>
 *   <li><b>Bootstraps</b> the two tracking tables ({@code fsm_schema_history} and
 *       {@code fsm_schema_lock}) by executing the dialect-specific {@code bootstrap.sql}
 *       resource. Errors indicating the tables already exist are silently ignored.</li>
 *   <li><b>Acquires an UPDATE-mutex lock</b> (portable, works without {@code SELECT ... FOR UPDATE})
 *       to prevent concurrent migrations across replicas.</li>
 *   <li><b>Applies</b> each migration version from {@link MigrationRegistry} that is not yet
 *       present in {@code fsm_schema_history}, in ascending order.</li>
 *   <li><b>Re-checks checksums</b> of already-applied versions. A mismatch warns by default;
 *       it hard-fails if {@code strictChecksum} is {@code true}. Checksums ignore line-ending
 *       differences (see {@link Migration#normalizeLineEndings}), so the same commit built on
 *       Windows and on Linux validates against the same database.</li>
 *   <li><b>Releases</b> the lock.</li>
 * </ol>
 *
 * <h2>Modes</h2>
 * <ul>
 *   <li>{@link MigrationMode#UPDATE} — full flow; schema is created/upgraded automatically.</li>
 *   <li>{@link MigrationMode#VALIDATE} — no lock, no DDL; fails fast if DB is behind and logs
 *       the exact pending SQL for operators to run out-of-band.</li>
 *   <li>{@link MigrationMode#OFF} — do nothing; caller manages the schema entirely.</li>
 * </ul>
 *
 * <h2>Lock mechanism</h2>
 * <p>The lock is a single-row UPDATE on {@code fsm_schema_lock}:
 * <pre>
 *   UPDATE fsm_schema_lock SET locked=1, locked_by=?, locked_at=?
 *   WHERE id=1 AND (locked=0 OR locked_at &lt; &lt;now - ttl&gt;)
 * </pre>
 * One affected row means the lock is held. Zero rows means another node holds it (or a
 * stale lock exists but has not yet expired). The migrator retries up to
 * {@code lockWaitTimeout} with 500 ms sleep intervals before failing with a descriptive
 * message. Stale locks (past {@code lockTtl}) are taken over automatically.
 *
 * <h2>Failure handling</h2>
 * <p>A migration's {@code fsm_schema_history} row is inserted <em>last</em>, only after all of
 * its DDL statements succeed, so a version is never recorded as applied unless it actually
 * completed. A failing migration throws and halts startup — a shipped migration that fails is a
 * bug to fix at the source (or an environment/permissions issue), so the migrator fails fast
 * rather than trying to recover. Because no history row is written on failure, the migration is
 * retried cleanly on the next startup once the cause is fixed.
 *
 * <h2>Thread safety</h2>
 * <p>This class is stateless after construction and safe to call from multiple threads,
 * though the distributed lock ensures only one migration runs at a time across replicas.
 */
public class SchemaMigrator {

    private static final Logger log = LoggerFactory.getLogger(SchemaMigrator.class);

    /**
     * Name of the history table managed by this migrator.
     */
    public static final String HISTORY_TABLE = "fsm_schema_history";

    /**
     * Name of the lock table managed by this migrator.
     */
    public static final String LOCK_TABLE = "fsm_schema_lock";

    /**
     * Lock retry interval.
     */
    private static final long LOCK_RETRY_MS = 500L;

    private final DataSource dataSource;
    private final SqlDialect dialect;
    private final MigrationMode mode;
    private final boolean strictChecksum;
    private final Duration lockTtl;
    private final Duration lockWaitTimeout;

    /**
     * Constructs a {@code SchemaMigrator} with the default {@link MigrationMode#UPDATE} mode
     * and sensible defaults for lock TTL (5 minutes) and wait timeout (30 seconds).
     *
     * @param dataSource the connection pool
     * @param dialect    the SQL dialect; used to resolve resource paths via {@link SqlDialect#id()}
     */
    public SchemaMigrator(DataSource dataSource, SqlDialect dialect) {
        this(dataSource, dialect, MigrationMode.UPDATE, false, Duration.ofMinutes(5), Duration.ofSeconds(30));
    }

    /**
     * Constructs a {@code SchemaMigrator} with full configuration.
     *
     * @param dataSource      the connection pool
     * @param dialect         the SQL dialect
     * @param mode            how the migrator should behave at startup
     * @param strictChecksum  {@code true} to fail on checksum mismatch; {@code false} to warn only
     * @param lockTtl         how long a lock is considered valid before being treated as stale
     * @param lockWaitTimeout how long to wait for the lock before giving up
     */
    public SchemaMigrator(DataSource dataSource,
                          SqlDialect dialect,
                          MigrationMode mode,
                          boolean strictChecksum,
                          Duration lockTtl,
                          Duration lockWaitTimeout) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.dialect = Objects.requireNonNull(dialect, "dialect must not be null");
        this.mode = Objects.requireNonNull(mode, "mode must not be null");
        this.strictChecksum = strictChecksum;
        this.lockTtl = Objects.requireNonNull(lockTtl, "lockTtl must not be null");
        this.lockWaitTimeout = Objects.requireNonNull(lockWaitTimeout, "lockWaitTimeout must not be null");
    }

    /**
     * Returns the configured {@link MigrationMode}.
     */
    public MigrationMode getMode() {
        return mode;
    }

    /**
     * Runs the migration flow according to the configured {@link MigrationMode}.
     *
     * <ul>
     *   <li>{@link MigrationMode#UPDATE} — bootstraps tables, acquires lock, applies pending
     *       migrations, verifies checksums, releases lock.</li>
     *   <li>{@link MigrationMode#VALIDATE} — bootstraps tables, compares history to registry,
     *       fails fast if any version is missing and logs the pending SQL.</li>
     *   <li>{@link MigrationMode#OFF} — returns immediately without touching the database.</li>
     * </ul>
     *
     * @throws MigrationException if any step fails
     */
    public void migrate() {
        switch (mode) {
            case OFF -> log.debug("[SchemaMigrator] mode=OFF — schema management skipped");
            case VALIDATE -> {
                log.info("[SchemaMigrator] mode=VALIDATE — verifying schema against registry");
                runBootstrap();
                runValidate();
            }
            case UPDATE -> {
                log.info("[SchemaMigrator] mode=UPDATE — dialect='{}', applying pending migrations", dialect.id());
                runBootstrap();
                String lockOwner = acquireLock();
                try {
                    Map<Integer, String> applied = readHistory();
                    applyGap(applied);
                    recheckChecksums(applied);
                } finally {
                    releaseLock(lockOwner);
                }
            }
        }
    }

    private void runBootstrap() {
        String bootstrapPath = "io/hypercell/fsm/db/migrations/" + dialect.id() + "/bootstrap.sql";
        String sql = loadResource(bootstrapPath);
        List<String> statements = splitStatements(sql);
        try (Connection conn = dataSource.getConnection()) {
            boolean autoCommit = conn.getAutoCommit();
            conn.setAutoCommit(true);
            try {
                for (String stmt : statements) {
                    try (Statement s = conn.createStatement()) {
                        s.execute(stmt);
                    } catch (SQLException e) {
                        if (isAlreadyExistsError(e)) {
                            log.debug("[SchemaMigrator] Bootstrap statement skipped (already exists): {}", e.getMessage());
                        } else {
                            throw new MigrationException("Bootstrap SQL failed: " + e.getMessage(), e);
                        }
                    }
                }
            } finally {
                conn.setAutoCommit(autoCommit);
            }
        } catch (MigrationException e) {
            throw e;
        } catch (SQLException e) {
            throw new MigrationException("Failed to obtain connection for bootstrap", e);
        }
        log.debug("[SchemaMigrator] Bootstrap complete for dialect='{}'", dialect.id());
    }

    private String acquireLock() {
        String lockOwner = buildLockOwner();
        long deadline = System.currentTimeMillis() + lockWaitTimeout.toMillis();

        while (true) {
            if (tryAcquireLock(lockOwner)) {
                log.debug("[SchemaMigrator] Acquired migration lock as '{}'", lockOwner);
                return lockOwner;
            }

            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new MigrationException(
                        "Could not acquire schema migration lock within " + lockWaitTimeout.toSeconds() + "s. " +
                                "Another node may be migrating, or a stale lock (TTL=" + lockTtl.toSeconds() + "s) " +
                                "exists in " + LOCK_TABLE + ". Investigate the 'locked_by' / 'locked_at' columns.");
            }

            log.debug("[SchemaMigrator] Lock held by another node; retrying in {}ms ({} ms remaining)",
                    LOCK_RETRY_MS, remaining);
            try {
                Thread.sleep(LOCK_RETRY_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new MigrationException("Interrupted while waiting for migration lock", e);
            }
        }
    }

    private boolean tryAcquireLock(String lockOwner) {
        String staleCutoff = Instant.now().minus(lockTtl).toString();
        String now = Instant.now().toString();

        String sql = "UPDATE " + LOCK_TABLE +
                " SET locked=1, locked_by=?, locked_at=?" +
                " WHERE id=1 AND (locked=0 OR locked_at < ?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, lockOwner);
            ps.setString(2, now);
            ps.setString(3, staleCutoff);
            int rows = ps.executeUpdate();
            return rows == 1;
        } catch (SQLException e) {
            throw new MigrationException("Failed to attempt lock acquisition: " + e.getMessage(), e);
        }
    }

    private void releaseLock(String lockOwner) {
        String sql = "UPDATE " + LOCK_TABLE + " SET locked=0 WHERE id=1 AND locked_by=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, lockOwner);
            int rows = ps.executeUpdate();
            if (rows == 0) {
                log.warn("[SchemaMigrator] Lock release: no row matched locked_by='{}' — " +
                        "lock may have been taken over (stale). Proceeding.", lockOwner);
            } else {
                log.debug("[SchemaMigrator] Released migration lock (locked_by='{}')", lockOwner);
            }
        } catch (SQLException e) {
            log.warn("[SchemaMigrator] Failed to release migration lock: {}", e.getMessage(), e);
        }
    }

    private String buildLockOwner() {
        String host;
        try {
            host = java.net.InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown-host";
        }
        return host + "@" + ProcessHandle.current().pid() + "#" + Thread.currentThread().getId();
    }

    /**
     * Reads all successfully applied versions from the history table.
     *
     * @return map of version → checksum for all {@code success=1} rows
     */
    private Map<Integer, String> readHistory() {
        String sql = "SELECT version, checksum FROM " + HISTORY_TABLE + " WHERE success=1";
        Map<Integer, String> history = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                history.put(rs.getInt("version"), rs.getString("checksum"));
            }
        } catch (SQLException e) {
            throw new MigrationException("Failed to read schema history: " + e.getMessage(), e);
        }
        log.debug("[SchemaMigrator] Applied versions in history: {}", history.keySet());
        return history;
    }

    private void applyGap(Map<Integer, String> applied) {
        for (Migration migration : MigrationRegistry.ALL) {
            if (applied.containsKey(migration.version())) {
                log.debug("[SchemaMigrator] V{} already applied — skipping", migration.version());
                continue;
            }
            applyMigration(migration);
        }
    }

    /**
     * Applies a single versioned migration: runs its DDL statements, then records it in
     * {@code fsm_schema_history}.
     *
     * <p>The history row is inserted <em>last</em>, only after every DDL statement has succeeded,
     * so a version is never marked as applied unless it actually completed. Statements that fail
     * with an "already exists" error (per {@link #isAlreadyExistsError}) are skipped. Any other
     * failure throws {@link MigrationException}, which halts startup — a shipped migration that
     * fails is a bug to fix at the source (or an environment/permissions problem), so the
     * migrator fails fast rather than attempting to recover. No history row is written on failure,
     * so the migration is retried cleanly once the cause is fixed.
     *
     * @param migration the migration to apply
     * @throws MigrationException if any statement fails (excluding "already exists" errors)
     */
    private void applyMigration(Migration migration) {
        log.info("[SchemaMigrator] Applying V{}  {}", migration.version(), migration.description());
        String sql = migration.loadSql(dialect.id());
        String checksum = Migration.checksum(sql);
        List<String> statements = splitStatements(sql);
        long started = System.currentTimeMillis();

        try (Connection conn = dataSource.getConnection()) {
            for (String stmt : statements) {
                try (Statement s = conn.createStatement()) {
                    s.execute(stmt);
                } catch (SQLException e) {
                    if (isAlreadyExistsError(e)) {
                        log.debug("[SchemaMigrator] DDL statement skipped (already exists): {}", e.getMessage());
                    } else {
                        throw new MigrationException(
                                "Failed to apply V" + migration.version()
                                        + " (" + migration.description() + "): " + e.getMessage(), e);
                    }
                }
            }
            long elapsed = System.currentTimeMillis() - started;
            recordHistory(conn, migration, checksum, elapsed);
            log.info("[SchemaMigrator] V{} applied in {}ms", migration.version(), elapsed);
        } catch (MigrationException e) {
            throw e;
        } catch (SQLException e) {
            throw new MigrationException("Failed to obtain connection for V" + migration.version(), e);
        }
    }

    /**
     * Inserts a {@code success=1} row into {@code fsm_schema_history}. Called only after a
     * migration's DDL has fully succeeded, so the table never records a version that did not
     * actually complete. No failure-marker ({@code success=0}) row is ever written — that would
     * collide with the {@code version} primary key when the migration is retried.
     */
    private void recordHistory(Connection conn, Migration migration, String checksum,
                               long elapsedMs) throws SQLException {
        String sql = "INSERT INTO " + HISTORY_TABLE +
                " (version, description, checksum, applied_at, execution_millis, success)" +
                " VALUES (?, ?, ?, ?, ?, ?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, migration.version());
            ps.setString(2, migration.description());
            ps.setString(3, checksum);
            ps.setString(4, Instant.now().toString());
            ps.setLong(5, elapsedMs);
            ps.setInt(6, 1);
            ps.executeUpdate();
        }
    }

    private void recheckChecksums(Map<Integer, String> applied) {
        for (Migration migration : MigrationRegistry.ALL) {
            String storedChecksum = applied.get(migration.version());
            if (storedChecksum == null) continue;
            String currentChecksum = Migration.checksum(migration.loadSql(dialect.id()));
            if (!currentChecksum.equals(storedChecksum)) {
                String msg = String.format(
                        "[SchemaMigrator] Checksum mismatch for V%d (%s): " +
                                "stored=%s, current=%s. " +
                                "The migration SQL file appears to have changed after it was applied.",
                        migration.version(), migration.description(), storedChecksum, currentChecksum);
                if (strictChecksum) {
                    throw new MigrationException(msg);
                } else {
                    log.warn(msg);
                }
            }
        }
    }

    private void runValidate() {
        Map<Integer, String> applied = readHistory();
        List<Migration> pending = new ArrayList<>();

        for (Migration migration : MigrationRegistry.ALL) {
            if (!applied.containsKey(migration.version())) {
                pending.add(migration);
            }
        }

        if (pending.isEmpty()) {
            log.info("[SchemaMigrator] VALIDATE: schema is up to date ({} version(s) applied)",
                    applied.size());
            return;
        }

        StringBuilder sb = new StringBuilder();
        sb.append("[SchemaMigrator] VALIDATE mode: schema is NOT up to date. ")
                .append(pending.size()).append(" pending migration(s):\n");

        for (Migration migration : pending) {
            sb.append("  V").append(migration.version())
                    .append(" (").append(migration.description()).append(")\n");
            try {
                String sql = migration.loadSql(dialect.id());
                sb.append("  -- SQL to apply:\n");
                for (String line : sql.split("\n")) {
                    sb.append("  ").append(line).append("\n");
                }
                sb.append("\n");
            } catch (MigrationException e) {
                sb.append("  [SQL file not found: ").append(e.getMessage()).append("]\n");
            }
        }

        String message = sb.toString();
        log.error(message);
        throw new MigrationException(
                "Schema is behind by " + pending.size() + " migration(s). " +
                        "Run the pending SQL manually or switch mode to UPDATE. " +
                        "Pending versions: " + pending.stream()
                        .map(m -> "V" + m.version()).toList());
    }

    private String loadResource(String path) {
        try (InputStream in = SchemaMigrator.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new MigrationException(
                        "Required classpath resource not found: '" + path + "'");
            }
            return Migration.normalizeLineEndings(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new MigrationException("Failed to read classpath resource '" + path + "'", e);
        }
    }

    /**
     * Splits a SQL script into individual statements on {@code ;} boundaries,
     * trimming and skipping blank / comment-only lines.
     *
     * <p>Comments are stripped <em>before</em> splitting on {@code ;}, so semicolons
     * that appear inside {@code --} comment text are never treated as statement terminators.
     *
     * @param sql the full script text
     * @return ordered list of non-blank SQL statements (without trailing semicolons)
     */
    static List<String> splitStatements(String sql) {
        String stripped = Arrays.stream(sql.split("\n"))
                .map(line -> {
                    int commentIdx = line.indexOf("--");
                    return commentIdx >= 0 ? line.substring(0, commentIdx) : line;
                })
                .collect(java.util.stream.Collectors.joining("\n"));

        List<String> result = new ArrayList<>();
        for (String part : stripped.split(";")) {
            String cleaned = part.trim();
            if (!cleaned.isEmpty()) {
                result.add(cleaned);
            }
        }
        return result;
    }

    /**
     * Heuristically determines whether a {@link SQLException} indicates that an object
     * (table, index, row) already exists — used to make bootstrap statements idempotent
     * on databases that do not support {@code IF NOT EXISTS} (e.g. older Oracle versions).
     *
     * @param e the exception to inspect
     * @return {@code true} if the error is a benign "already exists" condition
     */
    private static boolean isAlreadyExistsError(SQLException e) {
        String state = e.getSQLState();
        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        int code = e.getErrorCode();

        // ANSI SQL state for duplicate object
        if ("42S01".equals(state) || "X0Y32".equals(state)) return true;
        // Oracle: ORA-00955 name is already used, ORA-00001 unique constraint
        if (code == 955 || code == 1) return true;
        // Generic message heuristics
        return msg.contains("already exists") || msg.contains("table already")
                || msg.contains("duplicate key") || msg.contains("unique constraint");
    }
}
