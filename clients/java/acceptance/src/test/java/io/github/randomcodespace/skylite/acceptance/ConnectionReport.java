package io.github.randomcodespace.skylite.acceptance;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import javax.sql.DataSource;
import org.hibernate.boot.Metadata;
import org.hibernate.boot.spi.BootstrapContext;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.integrator.spi.Integrator;
import org.hibernate.jpa.boot.spi.IntegratorProvider;
import org.hibernate.jpa.boot.spi.JpaSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.hibernate.autoconfigure.HibernatePropertiesCustomizer;
import org.springframework.stereotype.Component;

/**
 * At each application start, checks that the application is connected to the database the run asked for, before
 * Hibernate creates, alters or drops anything, logs it, and records whether it is the baseline stack. Tests open
 * connections outside the pool only through {@link #openOutsidePool()}, from the same verified values.
 *
 * <p>The requested connection is the JVM system properties {@code jdbc.url}, {@code jdbc.driver} and
 * {@code jdbc.dialect}, which Surefire sets from the Maven properties of the same names (pom.xml). They are read
 * from the JVM, not from Spring's environment, where other sources (for example {@code SPRING_APPLICATION_JSON},
 * or {@code spring.*} system properties) can override what the application uses. The checks run in two steps:
 *
 * <ol>
 *   <li>when the pool is created, before it has opened a connection: it is HikariCP with the requested JDBC URL
 *       and driver class, not a {@code DataSource} class;
 *   <li>when Hibernate builds its session factory, before it processes the schema: Hibernate takes its
 *       connections from that pool, built the requested dialect, and the driver reports the requested URL.
 * </ol>
 *
 * Any difference fails start-up.
 */
@Component
class ConnectionReport implements BeanPostProcessor, HibernatePropertiesCustomizer {
    static final String BASELINE_DRIVER = "org.sqlite.JDBC";
    static final String BASELINE_DIALECT = "org.hibernate.community.dialect.SQLiteDialect";
    private static final Logger log = LoggerFactory.getLogger(ConnectionReport.class);

    private volatile HikariDataSource pool;
    private volatile Boolean baseline;

    @Override
    public Object postProcessAfterInitialization(Object bean, String name) {
        if (bean instanceof DataSource dataSource) {
            if (!(dataSource instanceof HikariDataSource hikari)) {
                throw new IllegalStateException("the DataSource is " + dataSource.getClass().getName() + ", not HikariCP");
            }
            List<String> differences = new ArrayList<>();
            if (hikari.getDataSourceClassName() != null || hikari.getDataSource() != null) {
                differences.add("HikariCP is configured with a DataSource instead of a driver");
            }
            compare(differences, "pool JDBC URL", requested("jdbc.url"), hikari.getJdbcUrl());
            compare(differences, "pool driver class", requested("jdbc.driver"), hikari.getDriverClassName());
            failOn(differences);
            pool = hikari;
        }
        return bean;
    }

    @Override
    public void customize(Map<String, Object> hibernateProperties) {
        Integrator check = new Integrator() {
            @Override
            public void integrate(Metadata metadata, BootstrapContext context, SessionFactoryImplementor sf) {
                verify(sf);
            }
        };
        hibernateProperties.put(JpaSettings.INTEGRATOR_PROVIDER, (IntegratorProvider) () -> List.of(check));
    }

    private void verify(SessionFactoryImplementor sf) {
        String url = requested("jdbc.url");
        String driver = requested("jdbc.driver");
        String dialect = requested("jdbc.dialect");
        List<String> differences = new ArrayList<>();
        ConnectionProvider provider = sf.getServiceRegistry().getService(ConnectionProvider.class);
        if (pool == null || provider == null || !provider.isUnwrappableAs(DataSource.class) || provider.unwrap(DataSource.class) != pool) {
            differences.add("Hibernate does not take its connections from the application's pool");
        }
        compare(differences, "Hibernate dialect", dialect, sf.getJdbcServices().getDialect().getClass().getName());
        failOn(differences);

        try (Connection c = pool.getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            compare(differences, "URL reported by the driver", url, md.getURL());
            failOn(differences);
            baseline = BASELINE_DRIVER.equals(driver) && BASELINE_DIALECT.equals(dialect);
            log.info("Connected to {} with driver {} ({} {}), dialect {}, database {} {}{}", md.getURL(), driver,
                    md.getDriverName(), md.getDriverVersion(), dialect, md.getDatabaseProductName(),
                    md.getDatabaseProductVersion(), baseline ? "; this is the baseline stack" : "");
        } catch (SQLException e) {
            throw new IllegalStateException("cannot read the connection's metadata", e);
        }
    }

    /** Whether the verified connection is the baseline stack; fails if start-up has not verified it. */
    boolean isBaseline() {
        Boolean b = baseline;
        if (b == null) {
            throw new IllegalStateException("the connection has not been verified");
        }
        return b;
    }

    /**
     * A new physical connection outside the pool, opened with the requested driver class and URL that start-up
     * verified, never with Spring's separately bound {@code spring.datasource.*} values. The pool's user name and
     * password, if any, are passed on; they do not choose the database.
     */
    Connection openOutsidePool() throws SQLException {
        isBaseline(); // fails unless start-up verified the connection
        String url = requested("jdbc.url");
        String driver = requested("jdbc.driver");
        Properties credentials = new Properties();
        if (pool.getUsername() != null) {
            credentials.setProperty("user", pool.getUsername());
        }
        if (pool.getPassword() != null) {
            credentials.setProperty("password", pool.getPassword());
        }
        Connection c;
        try {
            c = ((Driver) Class.forName(driver).getDeclaredConstructor().newInstance()).connect(url, credentials);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("cannot load driver " + driver, e);
        }
        if (c == null) {
            throw new IllegalStateException("driver " + driver + " does not accept " + url);
        }
        return c;
    }

    private static String requested(String property) {
        String value = System.getProperty(property);
        if (value == null || value.isBlank() || value.startsWith("${")) {
            throw new IllegalStateException(property + " is not set; run the suite through Maven with -D"
                    + property + " (see README.md)");
        }
        return value;
    }

    private static void compare(List<String> differences, String what, String requested, String actual) {
        if (!Objects.equals(requested, actual)) {
            differences.add(what + " is " + actual + ", requested " + requested);
        }
    }

    private static void failOn(List<String> differences) {
        if (!differences.isEmpty()) {
            throw new IllegalStateException("the application is not connected to the requested database: "
                    + String.join("; ", differences)
                    + ". Set the connection only with -Djdbc.url, -Djdbc.driver and -Djdbc.dialect.");
        }
    }
}
