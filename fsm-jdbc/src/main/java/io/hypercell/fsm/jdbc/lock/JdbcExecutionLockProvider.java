package io.hypercell.fsm.jdbc.lock;

import io.hypercell.fsm.exception.ConcurrentExecutionException;
import io.hypercell.fsm.jdbc.SqlDialect;
import io.hypercell.fsm.jdbc.migration.MigrationMode;
import io.hypercell.fsm.jdbc.migration.SchemaMigrator;
import io.hypercell.fsm.lock.ExecutionLockHandle;
import io.hypercell.fsm.lock.ExecutionLockProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/**
 * A distributed {@link ExecutionLockProvider} backed by the {@code fsm_execution_locks}
 * database table.
 *
 * <h2>How it works</h2>
 * <p>Each in-flight execution is represented by exactly one row in {@code fsm_execution_locks}.
 * Acquiring a lock is a two-step, non-blocking operation:
 * <ol>
 *   <li><b>UPDATE (stale takeover)</b> — if a row already exists but its {@code locked_at}
 *       timestamp is older than the configured TTL, the node claims it as stale:
 *       {@code UPDATE ... WHERE execution_id=? AND locked_at < now-ttl}.
 *       One affected row means the stale lock was taken over.</li>
 *   <li><b>INSERT (new or released)</b> — if 0 rows were updated, no active lock exists
 *       for this execution (either the row was never created, or it was deleted on release).
 *       An {@code INSERT} is attempted. Success means the lock is acquired. A duplicate-key
 *       error means another node raced to the same INSERT and won first — throw
 *       {@link ConcurrentExecutionException} immediately.</li>
 * </ol>
 * <p>Releasing <b>deletes</b> the row ({@code DELETE ... WHERE execution_id=? AND locked_by=?}).
 * The {@code AND locked_by=?} guard prevents a successfully-released node from accidentally
 * deleting a lock that was taken over as stale while it was still processing.
 * <p>If the owning process crashes, the row remains in the table; the next request for the
 * same execution will reclaim it via the UPDATE once the TTL has elapsed.
 *
 * <h2>Concurrency guarantee on first INSERT</h2>
 * <p>When two nodes simultaneously find 0 rows from the UPDATE (no row exists yet), both
 * attempt the INSERT. The database's primary key constraint guarantees that exactly one
 * INSERT succeeds; the other receives a duplicate-key error, which is converted to
 * {@link ConcurrentExecutionException}. No application-level coordination is required.
 *
 * <h2>Schema</h2>
 * <p>The {@code fsm_execution_locks} table is created automatically via migration V3 when a
 * {@link SchemaMigrator} is provided (the default). No manual DDL is required.
 *
 * <h2>Fail-fast semantics</h2>
 * <p>{@link #acquire(String)} never blocks. If the lock is held it throws
 * {@link ConcurrentExecutionException} immediately (maps to HTTP 409 Conflict), matching
 * the behaviour of the default
 * {@link io.hypercell.fsm.lock.ReentrantExecutionLockProvider}.
 *
 * <h2>Thread safety</h2>
 * <p>Stateless after construction; safe to share across threads.
 */
public class JdbcExecutionLockProvider implements ExecutionLockProvider {

    private static final Logger log = LoggerFactory.getLogger(JdbcExecutionLockProvider.class);

    public static final String TABLE = "fsm_execution_locks";

    private final DataSource dataSource;
    private final Duration lockTtl;

    /**
     * Create a provider with automatic schema migration in {@link MigrationMode#UPDATE} mode.
     *
     * @param dataSource the connection pool
     * @param dialect    the SQL dialect; used only for schema migration
     */
    public JdbcExecutionLockProvider(DataSource dataSource, SqlDialect dialect) {
        this(dataSource, Duration.ofMinutes(5),
                new SchemaMigrator(dataSource, dialect, MigrationMode.UPDATE, false,
                        Duration.ofMinutes(5), Duration.ofSeconds(30)));
    }

    /**
     * Create a provider with a pre-configured migrator.
     *
     * @param dataSource the connection pool
     * @param dialect    the SQL dialect (used only for migration; not for lock SQL itself)
     * @param lockTtl    how long a lock is considered valid before being treated as stale
     * @param migrator   the schema migrator; {@link SchemaMigrator#migrate()} is called during
     *                   construction. Pass a migrator with {@link MigrationMode#OFF} to skip
     *                   schema management (you must ensure the table exists yourself).
     */
    public JdbcExecutionLockProvider(DataSource dataSource, SqlDialect dialect,
                                     Duration lockTtl, SchemaMigrator migrator) {
        this(dataSource, lockTtl, migrator);
    }

    private JdbcExecutionLockProvider(DataSource dataSource, Duration lockTtl, SchemaMigrator migrator) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource must not be null");
        this.lockTtl = Objects.requireNonNull(lockTtl, "lockTtl must not be null");
        if (migrator != null) {
            migrator.migrate();
        }
    }

    /**
     * Acquire an exclusive lock for the given execution.
     * <p>
     * Non-blocking: returns immediately. Throws {@link ConcurrentExecutionException} if the
     * execution is actively locked by another thread or process.
     *
     * @param executionId the business entity ID to lock
     * @return a handle whose {@link ExecutionLockHandle#close()} deletes the lock row
     * @throws ConcurrentExecutionException if the lock is held by another request
     */
    @Override
    public ExecutionLockHandle acquire(String executionId) {
        String lockOwner = buildLockOwner();
        String staleCutoff = Instant.now().minus(lockTtl).toString();
        String now = Instant.now().toString();

        String updateSql = "UPDATE " + TABLE
                + " SET locked_by=?, locked_at=?"
                + " WHERE execution_id=? AND locked_at < ?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(updateSql)) {
            ps.setString(1, lockOwner);
            ps.setString(2, now);
            ps.setString(3, executionId);
            ps.setString(4, staleCutoff);
            if (ps.executeUpdate() == 1) {
                log.debug("[FsmLock] Claimed stale lock for '{}' as '{}'", executionId, lockOwner);
                return buildHandle(executionId, lockOwner);
            }
        } catch (SQLException e) {
            throw new RuntimeException(
                    "[FsmLock] Failed to attempt stale-lock takeover for '" + executionId + "'", e);
        }

        String insertSql = "INSERT INTO " + TABLE
                + " (execution_id, locked_by, locked_at) VALUES (?,?,?)";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(insertSql)) {
            ps.setString(1, executionId);
            ps.setString(2, lockOwner);
            ps.setString(3, now);
            ps.executeUpdate();
            log.debug("[FsmLock] Acquired lock for '{}' as '{}'", executionId, lockOwner);
            return buildHandle(executionId, lockOwner);
        } catch (SQLException e) {
            if (isDuplicateKeyError(e)) {
                throw new ConcurrentExecutionException(executionId);
            }
            throw new RuntimeException(
                    "[FsmLock] Failed to insert lock row for '" + executionId + "'", e);
        }
    }

    private ExecutionLockHandle buildHandle(String executionId, String lockOwner) {
        return () -> release(executionId, lockOwner);
    }

    private void release(String executionId, String lockOwner) {
        String sql = "DELETE FROM " + TABLE + " WHERE execution_id=? AND locked_by=?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, executionId);
            ps.setString(2, lockOwner);
            int rows = ps.executeUpdate();
            if (rows == 0) {
                log.warn("[FsmLock] Release: no row deleted for '{}' locked_by='{}' — "
                        + "lock may have been taken over as stale while processing. Proceeding.",
                        executionId, lockOwner);
            } else {
                log.debug("[FsmLock] Released lock for '{}'", executionId);
            }
        } catch (SQLException e) {
            log.warn("[FsmLock] Failed to release lock for '{}': {}", executionId, e.getMessage(), e);
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

    private static boolean isDuplicateKeyError(SQLException e) {
        String state = e.getSQLState();
        String msg = e.getMessage() != null ? e.getMessage().toLowerCase() : "";
        int code = e.getErrorCode();

        if ("23505".equals(state)) return true; // PostgreSQL, H2 unique_violation
        if ("23000".equals(state)) return true; // MySQL integrity_constraint_violation
        if ("23001".equals(state)) return true; // H2 unique_violation variant
        if (code == 1062) return true;           // MySQL: ER_DUP_ENTRY
        if (code == 1) return true;              // Oracle: ORA-00001
        if (code == 19) return true;             // SQLite: SQLITE_CONSTRAINT
        return msg.contains("unique constraint") || msg.contains("duplicate key")
                || msg.contains("duplicate entry") || msg.contains("unique index");
    }
}

