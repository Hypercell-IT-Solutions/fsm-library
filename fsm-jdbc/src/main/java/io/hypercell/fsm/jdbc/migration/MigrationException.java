package io.hypercell.fsm.jdbc.migration;

/**
 * Thrown by {@link SchemaMigrator} when schema migration cannot proceed.
 *
 * <p>Common causes:
 * <ul>
 *   <li>The database is behind the bundled registry while running in {@link MigrationMode#VALIDATE} mode.</li>
 *   <li>A migration SQL file is missing from the classpath for the active dialect.</li>
 *   <li>A checksum mismatch is detected and {@code strict-checksum} is enabled.</li>
 *   <li>The distributed lock cannot be acquired within the configured timeout.</li>
 * </ul>
 */
public class MigrationException extends RuntimeException {

    /**
     * Constructs a new {@code MigrationException} with the given message.
     *
     * @param message a human-readable description of the failure
     */
    public MigrationException(String message) {
        super(message);
    }

    /**
     * Constructs a new {@code MigrationException} with the given message and cause.
     *
     * @param message a human-readable description of the failure
     * @param cause   the underlying exception
     */
    public MigrationException(String message, Throwable cause) {
        super(message, cause);
    }
}
