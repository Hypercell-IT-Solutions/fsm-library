package io.hypercell.fsm.jdbc.migration;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

/**
 * Tests for {@link Migration} — resource resolution and, above all, the line-ending independence
 * of {@link Migration#checksum}.
 * <p>
 * A checksum answers "did this migration's SQL change?". Before normalization it also answered
 * "was this jar built on Windows or on Linux?", because the repository stores LF, a Windows
 * checkout with {@code core.autocrlf=true} materialises CRLF, and Maven copies resources into the
 * jar byte-for-byte. Two builds of the same commit therefore produced different checksums, and
 * upgrading between them failed against an already-migrated database.
 */
class MigrationTest {

    /**
     * Every dialect shipped in {@code fsm-jdbc/src/main/resources/io/hypercell/fsm/db/migrations/}.
     */
    private static final List<String> DIALECTS =
            List.of("h2", "mysql", "oracle", "postgresql", "sqlite");

    /**
     * V1 checksums as written by releases 1.0.0-RC1 through RC3, which were built from an LF
     * checkout. Normalizing to LF reproduces exactly these values, which is what lets a database
     * migrated by those releases keep validating. Pinning them here means an accidental change to
     * the normalization — or to V1 itself — fails loudly rather than silently invalidating every
     * deployed history table.
     */
    private static final Map<String, String> V1_CHECKSUMS = Map.of(
            "h2", "071693712307b48a35fdeadda667c150",
            "mysql", "d4bfb49e7658dc03eb5068e51fc81b28",
            "oracle", "be2707a9daf5aea66e4de2ca6064735d",
            "postgresql", "717578b72a5e1536342f80156afbf7a7",
            "sqlite", "773c299ca0e8f7250964fb5b241aef7c");

    private static final String LF_SQL = "CREATE TABLE t\n(\n    id INT\n);\n";

    @Test
    void checksum_isIdenticalForLfCrlfAndCr() {
        String crlf = LF_SQL.replace("\n", "\r\n");
        String cr = LF_SQL.replace("\n", "\r");

        assertThat(Migration.checksum(crlf))
                .as("CRLF must hash the same as LF")
                .isEqualTo(Migration.checksum(LF_SQL));
        assertThat(Migration.checksum(cr))
                .as("a lone CR must hash the same as LF")
                .isEqualTo(Migration.checksum(LF_SQL));
    }

    @Test
    void checksum_stillDiffersWhenTheSqlActuallyChanges() {
        String edited = LF_SQL.replace("id INT", "id BIGINT");

        assertThat(Migration.checksum(edited))
                .as("normalization must not blunt the guard it exists to keep useful")
                .isNotEqualTo(Migration.checksum(LF_SQL));
    }

    @Test
    void normalizeLineEndings_isIdempotent() {
        String once = Migration.normalizeLineEndings(LF_SQL.replace("\n", "\r\n"));

        assertThat(once).doesNotContain("\r");
        assertThat(Migration.normalizeLineEndings(once)).isEqualTo(once);
    }

    /**
     * The continuity guarantee: these are the values RC1–RC3 wrote into {@code fsm_schema_history}.
     */
    @Test
    void checksum_ofV1_matchesTheHistoricalLfValues() {
        for (String dialect : DIALECTS) {
            assertThat(Migration.checksum(MigrationRegistry.V1.loadSql(dialect)))
                    .as("V1 checksum for dialect '%s'", dialect)
                    .isEqualTo(V1_CHECKSUMS.get(dialect));
        }
    }

    /**
     * Guards the shipped resources themselves. Catches a CRLF file reaching the jar even if the
     * {@code .gitattributes} rule pinning {@code *.sql} to LF is later lost.
     */
    @Test
    void loadSql_neverReturnsCarriageReturns() {
        for (Migration migration : MigrationRegistry.ALL) {
            for (String dialect : DIALECTS) {
                assertThat(migration.loadSql(dialect))
                        .as("V%d for dialect '%s'", migration.version(), dialect)
                        .doesNotContain("\r");
            }
        }
    }

    @Test
    void checksum_isStableAcrossDialectsOnlyWhenTheSqlMatches() {
        String h2 = Migration.checksum(MigrationRegistry.V1.loadSql("h2"));
        String oracle = Migration.checksum(MigrationRegistry.V1.loadSql("oracle"));

        assertThat(h2)
                .as("the dialects carry genuinely different DDL, so their checksums must differ")
                .isNotEqualTo(oracle);
    }

    @Test
    void fileName_and_resourcePath_followTheVConvention() {
        assertThat(MigrationRegistry.V1.fileName()).isEqualTo("V1__create_snapshots.sql");
        assertThat(MigrationRegistry.V4.resourcePath("postgresql"))
                .isEqualTo("io/hypercell/fsm/db/migrations/postgresql/V4__add_failure_disposition.sql");
    }

    @Test
    void loadSql_throwsWithAHelpfulMessageForAnUnknownDialect() {
        assertThatThrownBy(() -> MigrationRegistry.V1.loadSql("no-such-db"))
                .isInstanceOf(MigrationException.class)
                .hasMessageContaining("no-such-db")
                .hasMessageContaining("V1__create_snapshots.sql");
    }
}
