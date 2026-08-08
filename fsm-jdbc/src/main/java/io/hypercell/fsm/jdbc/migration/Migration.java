package io.hypercell.fsm.jdbc.migration;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Immutable value type representing a single schema migration version.
 *
 * <p>A {@code Migration} is defined by its integer {@link #version()} and a
 * short {@link #description()} (lowercase, underscore-separated). Together they
 * form the canonical file name:
 * <pre>
 *   V{version}__{description}.sql
 *   e.g. V1__create_snapshots.sql
 * </pre>
 *
 * <p>The SQL content is resolved at runtime from the classpath under:
 * <pre>
 *   io/hypercell/fsm/db/migrations/{dialectId}/{filename}
 * </pre>
 *
 * <p>Checksums are computed as MD5 over the UTF-8 SQL content and represented
 * as lowercase hex strings. They are stored in {@code fsm_schema_history} to
 * detect post-apply file tampering.
 *
 * <p><strong>Line endings are normalized to {@code \n} before hashing.</strong> A checksum is meant
 * to answer "did this migration's SQL change?", and a CRLF/LF difference changes no SQL. Without
 * normalization the answer depends on the platform that built the jar — a Windows checkout with
 * {@code core.autocrlf=true} yields CRLF resources and therefore different checksums from a Linux
 * build of the very same commit, which makes an upgrade fail against a database migrated by the
 * other build. Normalizing makes the value a function of content alone.
 */
public record Migration(int version, String description) {

    /**
     * Returns the canonical SQL file name for this migration.
     *
     * @return e.g. {@code "V1__create_snapshots.sql"}
     */
    public String fileName() {
        return "V" + version + "__" + description + ".sql";
    }

    /**
     * Returns the classpath resource path for this migration for the given dialect.
     *
     * @param dialectId the dialect identifier returned by {@link io.hypercell.fsm.jdbc.SqlDialect#id()},
     *                  e.g. {@code "postgresql"}
     * @return e.g. {@code "io/hypercell/fsm/db/migrations/postgresql/V1__create_snapshots.sql"}
     */
    public String resourcePath(String dialectId) {
        return "io/hypercell/fsm/db/migrations/" + dialectId + "/" + fileName();
    }

    /**
     * Loads the SQL content for this migration from the classpath, with line endings normalized
     * to {@code \n}.
     *
     * @param dialectId the active dialect id
     * @return the full SQL text of the migration file
     * @throws MigrationException if the file cannot be found or read
     */
    public String loadSql(String dialectId) {
        String path = resourcePath(dialectId);
        try (InputStream in = Migration.class.getClassLoader().getResourceAsStream(path)) {
            if (in == null) {
                throw new MigrationException(
                        "Migration resource not found on classpath: '" + path + "'. " +
                                "Every registered migration version must have a SQL file for every supported dialect. " +
                                "Verify that the file exists in fsm-jdbc/src/main/resources/" + path);
            }
            return normalizeLineEndings(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new MigrationException("Failed to read migration resource '" + path + "'", e);
        }
    }

    /**
     * Rewrites CRLF and lone CR line endings to {@code \n}.
     *
     * <p>Applied on load and again before hashing, so the checksum of a migration depends on its
     * SQL and not on the platform that packaged the jar. Idempotent.
     *
     * @param sql the text to normalize
     * @return the text with {@code \r\n} and bare {@code \r} replaced by {@code \n}
     */
    public static String normalizeLineEndings(String sql) {
        return sql.replace("\r\n", "\n").replace('\r', '\n');
    }

    /**
     * Computes the MD5 checksum of the given SQL text, after
     * {@linkplain #normalizeLineEndings normalizing its line endings}.
     *
     * @param sql the SQL content to checksum
     * @return lowercase hex MD5 string, e.g. {@code "d41d8cd98f00b204e9800998ecf8427e"}
     */
    @SuppressWarnings("java:S4790") // MD5 detects accidental drift, not tampering by an attacker
    public static String checksum(String sql) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(normalizeLineEndings(sql).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("MD5 algorithm not available", e);
        }
    }
}
