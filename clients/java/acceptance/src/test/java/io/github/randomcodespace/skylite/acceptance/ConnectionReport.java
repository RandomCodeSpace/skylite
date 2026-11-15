package io.github.randomcodespace.skylite.acceptance;

import com.zaxxer.hikari.HikariDataSource;
import jakarta.persistence.EntityManagerFactory;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.sql.DataSource;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * At each application start, checks that the application is connected to the database the run asked for, logs
 * it, and records whether it is the baseline stack.
 *
 * <p>The requested connection is read from the JVM system properties that Surefire sets from {@code -Djdbc.url},
 * {@code -Djdbc.driver} and {@code -Djdbc.dialect} (pom.xml), and not from Spring's environment, which other
 * sources (for example {@code SPRING_APPLICATION_JSON}) can override. It is compared with what the application
 * actually uses: the URL the driver reports for a pooled connection, the driver class the pool loaded, the
 * dialect Hibernate built, and the pool Hibernate takes its connections from. Any difference fails start-up.
 */
@Component
class ConnectionReport implements ApplicationRunner {
    static final String BASELINE_DRIVER = "org.sqlite.JDBC";
    static final String BASELINE_DIALECT = "org.hibernate.community.dialect.SQLiteDialect";
    private static final Logger log = LoggerFactory.getLogger(ConnectionReport.class);

    private final DataSource dataSource;
    private final EntityManagerFactory emf;
    private volatile Boolean baseline;

    ConnectionReport(DataSource dataSource, EntityManagerFactory emf) {
        this.dataSource = dataSource;
        this.emf = emf;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        String url = requested("spring.datasource.url", "jdbc.url");
        String driver = requested("spring.datasource.driver-class-name", "jdbc.driver");
        String dialect = requested("spring.jpa.database-platform", "jdbc.dialect");

        List<String> differences = new ArrayList<>();
        if (!(dataSource instanceof HikariDataSource hikari)) {
            throw new IllegalStateException("the DataSource is " + dataSource.getClass().getName() + ", not HikariCP");
        }
        if (hikari.getDataSourceClassName() != null || hikari.getDataSource() != null) {
            differences.add("HikariCP is configured with a DataSource instead of a driver");
        }
        compare(differences, "pool JDBC URL", url, hikari.getJdbcUrl());
        compare(differences, "pool driver class", driver, hikari.getDriverClassName());

        SessionFactoryImplementor sf = emf.unwrap(SessionFactoryImplementor.class);
        compare(differences, "Hibernate dialect", dialect, sf.getJdbcServices().getDialect().getClass().getName());
        ConnectionProvider provider = sf.getServiceRegistry().getService(ConnectionProvider.class);
        if (provider == null || !provider.isUnwrappableAs(DataSource.class) || provider.unwrap(DataSource.class) != dataSource) {
            differences.add("Hibernate does not take its connections from the application's pool");
        }

        try (Connection c = dataSource.getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            compare(differences, "URL reported by the driver", url, md.getURL());
            if (!differences.isEmpty()) {
                throw new IllegalStateException("the application is not connected to the requested database: "
                        + String.join("; ", differences)
                        + ". Set the connection only with -Djdbc.url, -Djdbc.driver and -Djdbc.dialect.");
            }
            baseline = BASELINE_DRIVER.equals(driver) && BASELINE_DIALECT.equals(dialect);
            log.info("Connected to {} with driver {} ({} {}), dialect {}, database {} {}{}", md.getURL(), driver,
                    md.getDriverName(), md.getDriverVersion(), dialect, md.getDatabaseProductName(),
                    md.getDatabaseProductVersion(), baseline ? "; this is the baseline stack" : "");
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

    private static String requested(String systemProperty, String mavenProperty) {
        String value = System.getProperty(systemProperty);
        if (value == null || value.isBlank() || value.startsWith("${")) {
            throw new IllegalStateException(systemProperty + " is not set; run the suite through Maven with -D"
                    + mavenProperty + " (see README.md)");
        }
        return value;
    }

    private static void compare(List<String> differences, String what, String requested, String actual) {
        if (!Objects.equals(requested, actual)) {
            differences.add(what + " is " + actual + ", requested " + requested);
        }
    }
}
