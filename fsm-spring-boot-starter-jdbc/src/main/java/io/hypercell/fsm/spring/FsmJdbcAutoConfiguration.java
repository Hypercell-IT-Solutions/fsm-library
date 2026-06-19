package io.hypercell.fsm.spring;

import io.hypercell.fsm.jdbc.JdbcSnapshotRepository;
import io.hypercell.fsm.jdbc.SqlDialect;
import io.hypercell.fsm.jdbc.dialect.*;
import io.hypercell.fsm.jdbc.migration.SchemaMigrator;
import io.hypercell.fsm.resume.SnapshotRepository;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.sql.DataSource;

/**
 * Spring Boot autoconfiguration for FSM JDBC repository.
 *
 * <p>ENABLED BY DEFAULT. To disable:
 * <pre>{@code
 * fsm.jdbc.enabled: false
 * }</pre>
 *
 * <p>CUSTOMIZATION:
 * <pre>{@code
 * fsm:
 *   jdbc:
 *     dialect: postgresql|mysql|h2|sqlite|oracle
 *     migration:
 *       mode: UPDATE        # UPDATE (default), VALIDATE, or OFF
 *       strict-checksum: false
 *       lock-ttl: 5m
 *       lock-wait-timeout: 30s
 * }</pre>
 *
 * <p>OPTIONALITY: If you don't add this starter:
 * <ul>
 *   <li>Define your own {@code @Bean SnapshotRepository} if needed</li>
 *   <li>Or use {@code StateMachine.inMemoryRepository()}/{@code StateMachine.fileRepository()} directly</li>
 * </ul>
 *
 * <p>PRECEDENCE: If a {@code SnapshotRepository} bean already exists (user-defined),
 * this autoconfiguration will not create one ({@code @ConditionalOnMissingBean}).
 */
@AutoConfiguration
@ConditionalOnClass(JdbcSnapshotRepository.class)
@ConditionalOnProperty(
        name = "fsm.jdbc.enabled",
        havingValue = "true",
        matchIfMissing = true
)
@EnableConfigurationProperties(FsmJdbcProperties.class)
public class FsmJdbcAutoConfiguration {

    /**
     * Creates a {@link JdbcSnapshotRepository} bean if one doesn't already exist.
     *
     * <p>The repository is constructed with a {@link SchemaMigrator} built from the
     * configured {@link FsmJdbcProperties.Migration} settings. The migrator runs
     * <em>before</em> the repository is ready for use, ensuring the schema is in the
     * expected state on every application startup.
     *
     * @param dataSource Spring Boot autoconfigured DataSource
     * @param properties user configuration from application.yml / application.properties
     * @return a new {@link JdbcSnapshotRepository}, or skips if a bean already exists
     */
    @Bean
    @ConditionalOnMissingBean(SnapshotRepository.class)
    public SnapshotRepository snapshotRepository(DataSource dataSource, FsmJdbcProperties properties) {
        SqlDialect dialect = selectDialect(properties.getDialect());
        FsmJdbcProperties.Migration migrationProps = properties.getMigration();
        SchemaMigrator migrator = new SchemaMigrator(
                dataSource,
                dialect,
                migrationProps.getMode(),
                migrationProps.isStrictChecksum(),
                migrationProps.getLockTtl(),
                migrationProps.getLockWaitTimeout());
        return new JdbcSnapshotRepository(dataSource, dialect, migrator);
    }

    /**
     * Select the appropriate {@link SqlDialect} based on configuration.
     *
     * @param dialectName one of: postgresql (default), mysql, h2, sqlite, oracle
     * @return the corresponding dialect implementation
     * @throws IllegalArgumentException if the dialect is not recognized
     */
    private static SqlDialect selectDialect(String dialectName) {
        return switch (dialectName.toLowerCase()) {
            case "postgresql", "postgres" -> new PostgreSqlDialect();
            case "mysql", "mariadb" -> new MySqlDialect();
            case "h2" -> new H2Dialect();
            case "sqlite" -> new SqliteDialect();
            case "oracle" -> new OracleDialect();
            default -> throw new IllegalArgumentException(
                    "Unknown FSM JDBC dialect: '" + dialectName + "'. " +
                            "Supported: postgresql, mysql, h2, sqlite, oracle");
        };
    }
}
