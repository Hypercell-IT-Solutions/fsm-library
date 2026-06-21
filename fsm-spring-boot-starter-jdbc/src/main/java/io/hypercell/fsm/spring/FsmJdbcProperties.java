package io.hypercell.fsm.spring;

import io.hypercell.fsm.jdbc.migration.MigrationMode;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Duration;

/**
 * Configuration properties for FSM JDBC repository autoconfiguration.
 *
 * <p>EXAMPLE application.yml:
 * <pre>{@code
 * fsm:
 *   jdbc:
 *     enabled: true                    # enable/disable autoconfiguration
 *     dialect: postgresql              # postgresql, mysql, h2, sqlite, oracle
 *     migration:
 *       mode: UPDATE                   # UPDATE (default), VALIDATE, or OFF
 *       strict-checksum: true          # fail on checksum mismatch (false: warn only)
 *       lock-ttl: 5m                   # stale-lock TTL (default: 5 minutes)
 *       lock-wait-timeout: 30s         # how long to wait for the migration lock (default: 30s)
 * }</pre>
 *
 * <p>OPTIONALITY: This starter is OPTIONAL. You can use FSM without it:
 * <ul>
 *   <li>Use {@code StateMachine.inMemoryRepository()} directly — no beans needed</li>
 *   <li>Use {@code StateMachine.fileRepository(Path)} directly — no beans needed</li>
 *   <li>Define your own {@code @Bean SnapshotRepository} — autoconfiguration will skip</li>
 *   <li>Set {@code fsm.jdbc.enabled: false} — disables JDBC autoconfiguration</li>
 * </ul>
 */
@ConfigurationProperties(prefix = "fsm.jdbc")
public class FsmJdbcProperties {

    /**
     * Enable or disable JDBC autoconfiguration. Defaults to true.
     */
    private boolean enabled = true;

    /**
     * SQL dialect: postgresql (default), mysql, h2, sqlite, or oracle.
     */
    private String dialect = "postgresql";

    /**
     * Schema migration settings.
     */
    @NestedConfigurationProperty
    private Migration migration = new Migration();

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getDialect() {
        return dialect;
    }

    public void setDialect(String dialect) {
        this.dialect = dialect;
    }

    public Migration getMigration() {
        return migration;
    }

    public void setMigration(Migration migration) {
        this.migration = migration;
    }

    /**
     * Nested properties controlling the schema migration runner behaviour.
     */
    public static class Migration {

        /**
         * Migration mode: UPDATE (default), VALIDATE, or OFF.
         * <ul>
         *   <li>UPDATE — apply pending migrations automatically at startup.</li>
         *   <li>VALIDATE — fail fast if the DB schema is behind; log pending SQL for
         *       out-of-band execution.</li>
         *   <li>OFF — disable schema management entirely.</li>
         * </ul>
         */
        private MigrationMode mode = MigrationMode.UPDATE;

        /**
         * When {@code true}, a checksum mismatch on an already-applied migration causes
         * a hard failure. When {@code false} (default) a warning is logged instead.
         */
        private boolean strictChecksum = true;

        /**
         * How long the distributed migration lock is considered valid before being treated
         * as stale and taken over by another node. Defaults to 5 minutes.
         */
        private Duration lockTtl = Duration.ofMinutes(5);

        /**
         * How long to wait for the migration lock before aborting startup with an error.
         * Defaults to 30 seconds.
         */
        private Duration lockWaitTimeout = Duration.ofSeconds(30);

        public MigrationMode getMode() {
            return mode;
        }

        public void setMode(MigrationMode mode) {
            this.mode = mode;
        }

        public boolean isStrictChecksum() {
            return strictChecksum;
        }

        public void setStrictChecksum(boolean strictChecksum) {
            this.strictChecksum = strictChecksum;
        }

        public Duration getLockTtl() {
            return lockTtl;
        }

        public void setLockTtl(Duration lockTtl) {
            this.lockTtl = lockTtl;
        }

        public Duration getLockWaitTimeout() {
            return lockWaitTimeout;
        }

        public void setLockWaitTimeout(Duration lockWaitTimeout) {
            this.lockWaitTimeout = lockWaitTimeout;
        }
    }
}
