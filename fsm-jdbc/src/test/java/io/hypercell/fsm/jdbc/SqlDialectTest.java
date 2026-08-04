package io.hypercell.fsm.jdbc;

import io.hypercell.fsm.jdbc.dialect.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SqlDialectTest {

    @Test
    void h2Dialect_upsertUsesOnConflict() {
        String sql = new H2Dialect().upsertSql("fsm_snapshots").replaceAll("\\s+", " ");
        assertThat(sql).containsIgnoringCase("ON CONFLICT (execution_id) DO UPDATE");
        assertThat(sql).containsIgnoringCase("version = fsm_snapshots.version + 1");
    }

    @Test
    void postgreSqlDialect_upsertUsesOnConflict() {
        String sql = new PostgreSqlDialect().upsertSql("fsm_snapshots");
        assertThat(sql).containsIgnoringCase("ON CONFLICT (execution_id) DO UPDATE");
    }

    @Test
    void mySqlDialect_upsertUsesOnDuplicateKey() {
        String sql = new MySqlDialect().upsertSql("fsm_snapshots");
        assertThat(sql).containsIgnoringCase("ON DUPLICATE KEY UPDATE");
    }

    @Test
    void sqliteDialect_upsertUsesOnConflict() {
        String sql = new SqliteDialect().upsertSql("fsm_snapshots");
        assertThat(sql).containsIgnoringCase("ON CONFLICT");
    }

    @Test
    void oracleDialect_upsertUsesMerge() {
        String sql = new OracleDialect().upsertSql("fsm_snapshots");
        assertThat(sql).containsIgnoringCase("MERGE INTO");
        assertThat(sql).containsIgnoringCase("DUAL");
    }

    @Test
    void allDialects_haveStableId() {
        assertThat(new H2Dialect().id()).isEqualTo("h2");
        assertThat(new PostgreSqlDialect().id()).isEqualTo("postgresql");
        assertThat(new MySqlDialect().id()).isEqualTo("mysql");
        assertThat(new SqliteDialect().id()).isEqualTo("sqlite");
        assertThat(new OracleDialect().id()).isEqualTo("oracle");
    }

    @Test
    void limitClause_isDialectSpecific_oracleUsesFetchFirst() {
        // Oracle does not support LIMIT — it must use FETCH FIRST n ROWS ONLY.
        assertThat(new OracleDialect().limitClause(100)).isEqualTo("FETCH FIRST 100 ROWS ONLY");
        // The others use the standard LIMIT n form.
        for (SqlDialect dialect : List.of(
                new H2Dialect(), new PostgreSqlDialect(), new MySqlDialect(), new SqliteDialect())) {
            assertThat(dialect.limitClause(100))
                    .as("Dialect %s", dialect.getClass().getSimpleName())
                    .isEqualTo("LIMIT 100");
        }
    }

    @Test
    void upsertSql_uses15PositionalParameters() {
        // One placeholder per bound column in JdbcSnapshotRepository.bind(), which must agree
        // with every dialect's upsert. V4 added last_error_type and failure_disposition.
        for (SqlDialect dialect : List.of(
                new H2Dialect(), new PostgreSqlDialect(), new MySqlDialect(),
                new SqliteDialect(), new OracleDialect())) {
            String sql = dialect.upsertSql("fsm_snapshots");
            long count = sql.chars().filter(c -> c == '?').count();
            assertThat(count)
                    .as("Dialect %s should have 15 placeholders, got %d", dialect.getClass().getSimpleName(), count)
                    .isEqualTo(15);
        }
    }
}
