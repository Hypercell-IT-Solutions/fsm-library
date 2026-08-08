package io.hypercell.fsm.jdbc.migration;

import io.hypercell.fsm.jdbc.SqlDialect;
import io.hypercell.fsm.jdbc.dialect.H2Dialect;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.*;

/**
 * Dialect whose migration SQL resource path points to the "h2broken" test resources.
 * V1__create_snapshots.sql for this dialect contains a valid first statement followed
 * by intentionally invalid SQL, used to test that a mid-migration failure rolls back
 * the entire transaction (leaving no history row).
 */
class BrokenH2Dialect implements SqlDialect {
    @Override
    public String id() {
        return "h2broken";
    }

    @Override
    public String upsertSql(String tableName) {
        return "";
    }
}

/**
 * Tests for {@link SchemaMigrator} using an H2 in-memory database.
 */
class SchemaMigratorTest {

    @Test
    void freshDb_appliesFullChain() throws Exception {
        DataSource ds = newDs();
        SchemaMigrator migrator = migrator(ds);

        migrator.migrate();

        assertThat(historyVersions(ds)).containsExactly(1, 2, 3, 4);
        assertThat(tableExists(ds, "fsm_snapshots")).isTrue();
        assertThat(tableExists(ds, "fsm_execution_locks")).isTrue();
    }

    @Test
    void freshDb_v1AndV2_bothInHistory_andIndexExists() throws Exception {
        DataSource ds = newDs();
        migrator(ds).migrate();

        assertThat(historyVersions(ds)).containsExactly(1, 2, 3, 4);

        boolean indexFound = false;
        try (Connection conn = ds.getConnection();
             ResultSet rs = conn.getMetaData().getIndexInfo(null, null, "FSM_SNAPSHOTS", false, false)) {
            while (rs.next()) {
                String indexName = rs.getString("INDEX_NAME");
                if ("IDX_FSM_SNAPSHOTS_STATUS_ATTEMPT".equalsIgnoreCase(indexName)) {
                    indexFound = true;
                    break;
                }
            }
        }
        assertThat(indexFound).as("V2 index idx_fsm_snapshots_status_attempt must exist").isTrue();
    }

    @Test
    void secondRun_isNoOp() throws Exception {
        DataSource ds = newDs();
        SchemaMigrator migrator = migrator(ds);

        migrator.migrate();
        migrator.migrate();

        assertThat(historyVersions(ds)).containsExactly(1, 2, 3, 4);
    }

    @Test
    void preSeededAtV1_skipsV1() throws Exception {
        DataSource ds = newDs();
        SchemaMigrator migrator = migrator(ds);

        migrator.migrate();
        int initialCount = historyVersions(ds).size();

        migrator.migrate();
        assertThat(historyVersions(ds)).hasSize(initialCount);
    }

    @Test
    void checksumMismatch_warnsOnly_byDefault() throws Exception {
        DataSource ds = newDs();
        migrator(ds).migrate();

        updateChecksum(ds, 1, "badc0ffee");

        SchemaMigrator migrator = migrator(ds);
        assertThatNoException().isThrownBy(migrator::migrate);
    }

    @Test
    void checksumMismatch_failsUnderStrictChecksum() throws Exception {
        DataSource ds = newDs();
        migrator(ds).migrate();

        updateChecksum(ds, 1, "badc0ffee");

        SchemaMigrator strict = migratorStrict(ds);
        assertThatThrownBy(strict::migrate)
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("Checksum mismatch")
                .hasMessageContaining("V1");
    }

    /**
     * Re-running under the strict path must be clean: what {@code applyMigration} writes is what
     * {@code recheckChecksums} recomputes, on any platform, because both go through the
     * line-ending normalization in {@link Migration#checksum}.
     */
    @Test
    void reRun_underStrictChecksum_isClean() {
        DataSource ds = newDs();
        migratorStrict(ds).migrate();

        SchemaMigrator second = migratorStrict(ds);
        assertThatNoException().isThrownBy(second::migrate);
    }

    /**
     * Releases 1.0.0-RC4 and RC5 were packaged from a Windows checkout, so their jars carried CRLF
     * migration resources and stored CRLF checksums. Those values are no longer accepted — the
     * deliberate break documented in the RC6 release notes. RC1–RC3 databases are unaffected,
     * since normalizing to LF reproduces the checksums those releases wrote.
     */
    @Test
    void legacyCrlfChecksum_failsUnderStrictChecksum() throws Exception {
        DataSource ds = newDs();
        migratorStrict(ds).migrate();

        // The MD5 of h2/V1__create_snapshots.sql as it appeared, CRLF-encoded, in the RC4/RC5 jars.
        updateChecksum(ds, 1, "a944403b15733b3167a95bf897ac4302");

        SchemaMigrator strict = migratorStrict(ds);
        assertThatThrownBy(strict::migrate)
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("Checksum mismatch")
                .hasMessageContaining("V1");
    }

    @Test
    void validateMode_emptyDb_failsFast() {
        DataSource ds = newDs();

        SchemaMigrator updater = migrator(ds);
        updater.migrate();
        deleteHistory(ds);

        SchemaMigrator validator = new SchemaMigrator(ds, new H2Dialect(), MigrationMode.VALIDATE,
                false, Duration.ofMinutes(5), Duration.ofSeconds(30));

        assertThatThrownBy(validator::migrate)
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("behind")
                .hasMessageContaining("V1");
    }

    @Test
    void validateMode_upToDate_passes() {
        DataSource ds = newDs();
        migrator(ds).migrate();

        SchemaMigrator validator = new SchemaMigrator(ds, new H2Dialect(), MigrationMode.VALIDATE,
                false, Duration.ofMinutes(5), Duration.ofSeconds(30));

        assertThatNoException().isThrownBy(validator::migrate);
    }

    @Test
    void offMode_doesNothing() {
        DataSource ds = newDs();

        SchemaMigrator off = new SchemaMigrator(ds, new H2Dialect(), MigrationMode.OFF,
                false, Duration.ofMinutes(5), Duration.ofSeconds(30));

        assertThatNoException().isThrownBy(off::migrate);
        assertThat(tableExists(ds, "fsm_schema_history")).isFalse();
    }

    @Test
    void heldLock_waitsAndTimesOut() throws Exception {
        DataSource ds = newDs();
        migrator(ds).migrate();

        holdLock(ds, "external-node", Instant.now().toString());

        SchemaMigrator migrator = new SchemaMigrator(ds, new H2Dialect(), MigrationMode.UPDATE,
                false, Duration.ofMinutes(5), Duration.ofMillis(200));

        assertThatThrownBy(migrator::migrate)
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("lock");
    }

    @Test
    void staleLock_isTakenOver() throws Exception {
        DataSource ds = newDs();
        migrator(ds).migrate();

        String staleTime = Instant.now().minus(Duration.ofHours(1)).toString();
        holdLock(ds, "dead-node", staleTime);

        SchemaMigrator migrator = new SchemaMigrator(ds, new H2Dialect(), MigrationMode.UPDATE,
                false, Duration.ofMinutes(5), Duration.ofSeconds(5));

        assertThatNoException().isThrownBy(migrator::migrate);
    }

    @Test
    void registry_everyVersionHasFileForEveryDialect() {
        List<SqlDialect> dialects = List.of(
                new H2Dialect(),
                new io.hypercell.fsm.jdbc.dialect.PostgreSqlDialect(),
                new io.hypercell.fsm.jdbc.dialect.MySqlDialect(),
                new io.hypercell.fsm.jdbc.dialect.SqliteDialect(),
                new io.hypercell.fsm.jdbc.dialect.OracleDialect()
        );

        for (Migration migration : MigrationRegistry.ALL) {
            for (SqlDialect dialect : dialects) {
                String path = migration.resourcePath(dialect.id());
                assertThat(SchemaMigratorTest.class.getClassLoader().getResourceAsStream(path))
                        .as("Migration file missing for dialect='%s', version=V%d: %s",
                                dialect.id(), migration.version(), path)
                        .isNotNull();
            }
        }
    }

    @Test
    void registry_bootstrapExistsForEveryDialect() {
        List<SqlDialect> dialects = List.of(
                new H2Dialect(),
                new io.hypercell.fsm.jdbc.dialect.PostgreSqlDialect(),
                new io.hypercell.fsm.jdbc.dialect.MySqlDialect(),
                new io.hypercell.fsm.jdbc.dialect.SqliteDialect(),
                new io.hypercell.fsm.jdbc.dialect.OracleDialect()
        );

        for (SqlDialect dialect : dialects) {
            String path = "io/hypercell/fsm/db/migrations/" + dialect.id() + "/bootstrap.sql";
            assertThat(SchemaMigratorTest.class.getClassLoader().getResourceAsStream(path))
                    .as("bootstrap.sql missing for dialect='%s': %s", dialect.id(), path)
                    .isNotNull();
        }
    }

    @Test
    void splitStatements_handlesCommentsAndBlanks() {
        String sql = """
                -- comment
                CREATE TABLE foo (id INT);
                
                -- another comment
                CREATE INDEX idx_foo ON foo (id);
                """;
        List<String> stmts = SchemaMigrator.splitStatements(sql);
        assertThat(stmts).hasSize(2);
        assertThat(stmts.get(0)).containsIgnoringCase("CREATE TABLE");
        assertThat(stmts.get(1)).containsIgnoringCase("CREATE INDEX");
    }

    @Test
    void splitStatements_semicolonInsideComment_notTreatedAsTerminator() {
        String sql = """
                -- Seed row (id=1); ignore if already exists
                MERGE INTO foo (id) KEY (id) VALUES (1);
                """;
        List<String> stmts = SchemaMigrator.splitStatements(sql);
        assertThat(stmts).hasSize(1);
        assertThat(stmts.get(0)).containsIgnoringCase("MERGE INTO");
    }

    /**
     * Verifies the explicit-transaction guarantee: when a migration fails partway through
     * (the h2broken dialect's V1 has a valid first statement then deliberately invalid SQL),
     * the entire transaction — including the history INSERT — is rolled back, so no row
     * for that version appears in {@code fsm_schema_history}.
     *
     * <p>We do NOT assert DDL rollback of the table itself because H2's DDL-commit behaviour
     * is not guaranteed; the history-table assertion is the database-agnostic guarantee.
     */
    @Test
    void failedMigration_noHistoryRowWritten() {
        DataSource ds = newDs();

        SchemaMigrator migrator = new SchemaMigrator(ds, new BrokenH2Dialect());

        assertThatThrownBy(migrator::migrate)
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("V1");

        assertThat(historyVersionsAll(ds))
                .as("No history row should exist after a rolled-back migration")
                .isEmpty();
    }

    private static final AtomicInteger DB_COUNTER = new AtomicInteger(0);

    private static DataSource newDs() {
        JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:fsm_migration_test_" + DB_COUNTER.incrementAndGet() + ";DB_CLOSE_DELAY=-1");
        ds.setUser("sa");
        ds.setPassword("");
        return ds;
    }

    private static SchemaMigrator migrator(DataSource ds) {
        return new SchemaMigrator(ds, new H2Dialect());
    }

    private static SchemaMigrator migratorStrict(DataSource ds) {
        return new SchemaMigrator(ds, new H2Dialect(), MigrationMode.UPDATE,
                true, Duration.ofMinutes(5), Duration.ofSeconds(30));
    }

    private static List<Integer> historyVersions(DataSource ds) throws Exception {
        List<Integer> versions = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT version FROM fsm_schema_history WHERE success=1 ORDER BY version")) {
            while (rs.next()) {
                versions.add(rs.getInt(1));
            }
        }
        return versions;
    }

    private static List<Integer> historyVersionsAll(DataSource ds) {
        List<Integer> versions = new ArrayList<>();
        try (Connection conn = ds.getConnection();
             Statement s = conn.createStatement();
             ResultSet rs = s.executeQuery(
                     "SELECT version FROM fsm_schema_history ORDER BY version")) {
            while (rs.next()) {
                versions.add(rs.getInt(1));
            }
        } catch (Exception e) {
            return versions;
        }
        return versions;
    }

    private static boolean tableExists(DataSource ds, String tableName) {
        try (Connection conn = ds.getConnection();
             ResultSet rs = conn.getMetaData().getTables(null, null, tableName.toUpperCase(), null)) {
            return rs.next();
        } catch (Exception e) {
            return false;
        }
    }

    private static void updateChecksum(DataSource ds, int version, String newChecksum) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE fsm_schema_history SET checksum=? WHERE version=?")) {
            ps.setString(1, newChecksum);
            ps.setInt(2, version);
            ps.executeUpdate();
        }
    }

    private static void deleteHistory(DataSource ds) {
        try (Connection conn = ds.getConnection();
             Statement s = conn.createStatement()) {
            s.execute("DELETE FROM fsm_schema_history");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void holdLock(DataSource ds, String lockedBy, String lockedAt) throws Exception {
        try (Connection conn = ds.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE fsm_schema_lock SET locked=1, locked_by=?, locked_at=? WHERE id=1")) {
            ps.setString(1, lockedBy);
            ps.setString(2, lockedAt);
            ps.executeUpdate();
        }
    }
}
