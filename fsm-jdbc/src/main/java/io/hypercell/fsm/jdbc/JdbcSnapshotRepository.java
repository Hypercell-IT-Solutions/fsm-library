package io.hypercell.fsm.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.hypercell.fsm.core.ActionResult;
import io.hypercell.fsm.exception.SnapshotException;
import io.hypercell.fsm.failure.FailureDisposition;
import io.hypercell.fsm.jdbc.migration.MigrationMode;
import io.hypercell.fsm.jdbc.migration.SchemaMigrator;
import io.hypercell.fsm.resume.ExecutionSnapshot;
import io.hypercell.fsm.resume.SnapshotRepository;
import io.hypercell.fsm.resume.SnapshotStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

/**
 * A {@link SnapshotRepository} backed by any SQL database via plain JDBC.
 *
 * <h2>Automatic schema management</h2>
 * <p>By default this repository runs {@link SchemaMigrator} in {@link MigrationMode#UPDATE}
 * mode at construction time, creating or upgrading the schema automatically. No manual DDL
 * step is required — just provide a {@link DataSource} and a {@link SqlDialect} for your
 * database.
 *
 * <p>To opt out of automatic DDL, supply a pre-configured {@link SchemaMigrator}:
 * <ul>
 *   <li>{@link MigrationMode#VALIDATE} — fails fast if the DB is behind the bundled registry
 *       and logs the pending SQL for operators to apply out-of-band.</li>
 *   <li>{@link MigrationMode#OFF} — schema management is disabled entirely; the caller is
 *       responsible for the schema.</li>
 * </ul>
 *
 * <h2>Usage</h2>
 * <pre>{@code
 * DataSource dataSource = ...; // your connection pool (HikariCP, etc.)
 * // Table is created/migrated automatically on first instantiation:
 * SnapshotRepository repo = new JdbcSnapshotRepository(dataSource, new PostgreSqlDialect());
 * }</pre>
 *
 * <h2>Thread safety</h2>
 * <p>Thread-safe. Each operation acquires and releases a connection from the pool independently.
 *
 * <h2>Optimistic locking</h2>
 * <p>The {@code version} column is incremented on every save. The upsert is atomic at the
 * database level, preventing lost updates within a single database instance.
 *
 * <h2>Serialization</h2>
 * <p>{@code completedSubStepResults} is stored as JSON in the {@code completed_steps} column
 * in a direct key-value format: {@code {subStepName: {status, error?, output}, ...}}.
 * All timestamp fields are stored as ISO-8601 strings to avoid timezone issues.
 */
public class JdbcSnapshotRepository implements SnapshotRepository {

    private static final Logger log = LoggerFactory.getLogger(JdbcSnapshotRepository.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * The snapshot table name used by this repository.
     */
    public static final String DEFAULT_TABLE = "fsm_snapshots";

    private static final String SELECT_COLUMNS = """
            execution_id, machine_definition_id, current_state_name, failed_state_name,
            failed_sub_step_name, last_trigger_event, attempt_number, last_failed_at,
            scheduled_retry_at, last_error_message, last_error_type, failure_disposition,
            status, captured_at, completed_steps
            """;

    private final DataSource dataSource;
    private final SqlDialect dialect;

    private final String selectSql;
    private final String deleteSql;
    private final String listPendingSql;
    private final String upsertSql;

    /**
     * Create a repository targeting the {@value #DEFAULT_TABLE} table.
     * The schema is created/migrated automatically in {@link MigrationMode#UPDATE} mode.
     *
     * @param dataSource the connection pool; must be pre-configured and ready
     * @param dialect    the database-specific upsert strategy
     */
    public JdbcSnapshotRepository(DataSource dataSource, SqlDialect dialect) {
        this(dataSource, dialect, new SchemaMigrator(dataSource, dialect,
                MigrationMode.UPDATE, false, Duration.ofMinutes(5), Duration.ofSeconds(30)));
    }

    /**
     * Create a repository with a fully configured {@link SchemaMigrator}.
     * Use this constructor when you want to control the migration mode (VALIDATE, OFF, or UPDATE
     * with custom TTL / strict-checksum settings).
     *
     * @param dataSource     the connection pool
     * @param dialect        the database-specific upsert strategy
     * @param schemaMigrator the pre-configured migrator; {@link SchemaMigrator#migrate()} is
     *                       called during construction
     */
    public JdbcSnapshotRepository(DataSource dataSource,
                                  SqlDialect dialect,
                                  SchemaMigrator schemaMigrator) {
        this.dataSource = dataSource;
        this.dialect = dialect;
        this.selectSql = "SELECT " + SELECT_COLUMNS + " FROM " + DEFAULT_TABLE + " WHERE execution_id = ?";
        this.deleteSql = "DELETE FROM " + DEFAULT_TABLE + " WHERE execution_id = ?";
        this.listPendingSql = "SELECT " + SELECT_COLUMNS + " FROM " + DEFAULT_TABLE
                + " WHERE status IN ('FAILED', 'RETRY_SCHEDULED')";
        this.upsertSql = dialect.upsertSql(DEFAULT_TABLE);
        schemaMigrator.migrate();
    }

    @Override
    public void save(String executionId, ExecutionSnapshot snapshot) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(upsertSql)) {
            bind(ps, snapshot);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new SnapshotException("Failed to save snapshot for '" + executionId + "'", e);
        }
    }

    @Override
    public Optional<ExecutionSnapshot> load(String executionId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(selectSql)) {
            ps.setString(1, executionId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? Optional.of(toSnapshot(rs)) : Optional.empty();
            }
        } catch (SQLException e) {
            throw new SnapshotException("Failed to load snapshot for '" + executionId + "'", e);
        }
    }

    @Override
    public void delete(String executionId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(deleteSql)) {
            ps.setString(1, executionId);
            ps.executeUpdate();
        } catch (SQLException e) {
            log.error("[JdbcSnapshotRepository] Could not delete snapshot for '{}': {}",
                    executionId, e.getMessage(), e);
        }
    }

    @Override
    public List<ExecutionSnapshot> listPendingRetries() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(listPendingSql);
             ResultSet rs = ps.executeQuery()) {
            List<ExecutionSnapshot> results = new ArrayList<>();
            while (rs.next()) {
                results.add(toSnapshot(rs));
            }
            return results;
        } catch (SQLException e) {
            throw new SnapshotException("Failed to list pending retries", e);
        }
    }

    /**
     * Return a keyset-paginated page of interrupted executions (status = {@code RUNNING}).
     * <p>
     * SQL: {@code WHERE status = 'RUNNING' AND (? IS NULL OR execution_id > ?)
     * ORDER BY execution_id LIMIT ?}
     * <p>
     * The first-page call passes {@code afterExecutionId = null}; subsequent pages pass the
     * last execution ID seen. The row-limiting clause is dialect-specific via
     * {@link SqlDialect#limitClause(int)} ({@code LIMIT n} for PostgreSQL/MySQL/H2/SQLite,
     * {@code FETCH FIRST n ROWS ONLY} for Oracle).
     */
    @Override
    public List<ExecutionSnapshot> listInterrupted(int limit, String afterExecutionId) {
        String limitClause = dialect.limitClause(limit);
        String sql;
        if (afterExecutionId == null) {
            sql = "SELECT " + SELECT_COLUMNS + " FROM " + DEFAULT_TABLE
                    + " WHERE status = 'RUNNING'"
                    + " ORDER BY execution_id"
                    + " " + limitClause;
        } else {
            sql = "SELECT " + SELECT_COLUMNS + " FROM " + DEFAULT_TABLE
                    + " WHERE status = 'RUNNING' AND execution_id > ?"
                    + " ORDER BY execution_id"
                    + " " + limitClause;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            if (afterExecutionId != null) {
                ps.setString(1, afterExecutionId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<ExecutionSnapshot> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(toSnapshot(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new SnapshotException("Failed to list interrupted executions", e);
        }
    }

    /**
     * Return a keyset-paginated page of failed executions eligible for a consumer-driven
     * retry sweep (status = {@code FAILED} and {@code attempt_number < maxAttempts}).
     * <p>
     * SQL (first page): {@code WHERE status = 'FAILED' AND attempt_number < ?
     * ORDER BY execution_id <limitClause>}
     * <p>
     * SQL (keyset page): {@code WHERE status = 'FAILED' AND attempt_number < ?
     * AND execution_id > ? ORDER BY execution_id <limitClause>}
     * <p>
     * The row-limiting clause is dialect-specific via {@link SqlDialect#limitClause(int)}.
     */
    @Override
    public List<ExecutionSnapshot> listFailed(int limit, String afterExecutionId, int maxAttempts) {
        String limitClause = dialect.limitClause(limit);
        String sql;
        if (afterExecutionId == null) {
            sql = "SELECT " + SELECT_COLUMNS + " FROM " + DEFAULT_TABLE
                    + " WHERE status = 'FAILED' AND failure_disposition = 'RETRY'"
                    + " AND attempt_number < ?"
                    + " ORDER BY execution_id"
                    + " " + limitClause;
        } else {
            sql = "SELECT " + SELECT_COLUMNS + " FROM " + DEFAULT_TABLE
                    + " WHERE status = 'FAILED' AND failure_disposition = 'RETRY'"
                    + " AND attempt_number < ? AND execution_id > ?"
                    + " ORDER BY execution_id"
                    + " " + limitClause;
        }
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setInt(1, maxAttempts);
            if (afterExecutionId != null) {
                ps.setString(2, afterExecutionId);
            }
            try (ResultSet rs = ps.executeQuery()) {
                List<ExecutionSnapshot> results = new ArrayList<>();
                while (rs.next()) {
                    results.add(toSnapshot(rs));
                }
                return results;
            }
        } catch (SQLException e) {
            throw new SnapshotException("Failed to list failed executions", e);
        }
    }

    private static void bind(PreparedStatement ps, ExecutionSnapshot s) throws SQLException {
        ps.setString(1, s.getExecutionId());
        ps.setString(2, s.getMachineDefinitionId());
        ps.setString(3, s.getCurrentStateName());
        ps.setString(4, s.getFailedStateName());
        ps.setString(5, s.getFailedSubStepName());
        ps.setString(6, s.getLastTriggerEvent());
        ps.setInt(7, s.getAttemptNumber());
        ps.setString(8, s.getLastFailedAt() != null ? s.getLastFailedAt().toString() : null);
        ps.setString(9, s.getScheduledRetryAt() != null ? s.getScheduledRetryAt().toString() : null);
        ps.setString(10, s.getLastErrorMessage());
        ps.setString(11, s.getLastErrorType());
        ps.setString(12, s.getFailureDisposition().name());
        ps.setString(13, s.getStatus().name());
        ps.setString(14, s.getCapturedAt().toString());
        ps.setString(15, serializeSteps(s.getCompletedSubStepResults()));
    }

    private static ExecutionSnapshot toSnapshot(ResultSet rs) throws SQLException {
        return new ExecutionSnapshot.Builder()
                .executionId(rs.getString("execution_id"))
                .machineDefinitionId(rs.getString("machine_definition_id"))
                .currentStateName(rs.getString("current_state_name"))
                .failedStateName(rs.getString("failed_state_name"))
                .failedSubStepName(rs.getString("failed_sub_step_name"))
                .lastTriggerEvent(rs.getString("last_trigger_event"))
                .attemptNumber(rs.getInt("attempt_number"))
                .lastFailedAt(parseInstant(rs.getString("last_failed_at")))
                .scheduledRetryAt(parseInstant(rs.getString("scheduled_retry_at")))
                .lastErrorMessage(rs.getString("last_error_message"))
                .lastErrorType(rs.getString("last_error_type"))
                .failureDisposition(parseDisposition(rs.getString("failure_disposition")))
                .status(SnapshotStatus.valueOf(rs.getString("status")))
                .capturedAt(parseInstant(rs.getString("captured_at")))
                .completedSubStepResults(deserializeSteps(rs.getString("completed_steps")))
                .build();
    }

    static String serializeSteps(Map<String, ActionResult> results) {
        if (results.isEmpty()) return "";
        try {
            ObjectNode root = objectMapper.createObjectNode();
            for (Map.Entry<String, ActionResult> entry : results.entrySet()) {
                ObjectNode stepNode = objectMapper.createObjectNode();
                ActionResult ar = entry.getValue();
                stepNode.put("status", ar.getStatus().name());
                if (ar.getErrorMessage() != null) {
                    stepNode.put("error", ar.getErrorMessage());
                }
                ObjectNode outputNode = objectMapper.createObjectNode();
                for (Map.Entry<String, Object> out : ar.getOutput().entrySet()) {
                    outputNode.put(out.getKey(), out.getValue() != null ? out.getValue().toString() : "");
                }
                stepNode.set("output", outputNode);
                root.set(entry.getKey(), stepNode);
            }
            return objectMapper.writeValueAsString(root);
        } catch (JsonProcessingException e) {
            throw new SnapshotException("Failed to serialize completed steps", e);
        }
    }

    static Map<String, ActionResult> deserializeSteps(String text) {
        if (text == null || text.isBlank()) return Collections.emptyMap();
        try {
            JsonNode root = objectMapper.readTree(text);
            Map<String, ActionResult> result = new LinkedHashMap<>();
            root.fields().forEachRemaining(field -> {
                String subStepName = field.getKey();
                JsonNode stepNode = field.getValue();
                String status = stepNode.get("status").asText();
                String error = stepNode.has("error") ? stepNode.get("error").asText() : null;
                JsonNode outputNode = stepNode.get("output");
                Map<String, Object> output = new LinkedHashMap<>();
                outputNode.fields().forEachRemaining(outField ->
                        output.put(outField.getKey(), outField.getValue().asText())
                );
                ActionResult ar;
                if ("FAILED".equals(status)) {
                    ar = error != null ? ActionResult.failed(error) : ActionResult.failed("unknown");
                } else if ("SKIPPED".equals(status)) {
                    ar = ActionResult.skipped();
                } else {
                    ar = output.isEmpty() ? ActionResult.success() : ActionResult.success(output);
                }
                result.put(subStepName, ar);
            });
            return result;
        } catch (JsonProcessingException e) {
            throw new SnapshotException("Failed to deserialize completed steps", e);
        }
    }

    private static Instant parseInstant(String s) {
        if (s == null || s.isBlank()) return null;
        try {
            return Instant.parse(s);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Read the {@code failure_disposition} column, tolerating anything unexpected.
     * <p>
     * The column is {@code NOT NULL DEFAULT 'RETRY'}, so in practice a value is always present.
     * An unreadable one — a row written by a newer version of the library that added a
     * disposition this one does not know — falls back to {@code RETRY} rather than failing the
     * whole query, since {@code RETRY} is the behaviour the library had before dispositions and
     * is the safe reading of "we don't know".
     */
    private static FailureDisposition parseDisposition(String s) {
        if (s == null || s.isBlank()) return FailureDisposition.RETRY;
        try {
            return FailureDisposition.valueOf(s);
        } catch (IllegalArgumentException e) {
            log.warn("[JdbcSnapshotRepository] Unknown failure_disposition '{}'; treating as RETRY", s);
            return FailureDisposition.RETRY;
        }
    }
}
