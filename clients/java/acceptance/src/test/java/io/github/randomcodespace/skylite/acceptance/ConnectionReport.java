package io.github.randomcodespace.skylite.acceptance;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

/** At each application start, logs the database the suite is connected to, and refuses settings it would ignore. */
@Component
class ConnectionReport implements ApplicationRunner {
    private static final Logger log = LoggerFactory.getLogger(ConnectionReport.class);
    // Surefire passes the connection settings as system properties, which outrank these environment variables.
    private static final List<String> IGNORED_ENVIRONMENT = List.of(
            "SPRING_DATASOURCE_URL", "SPRING_DATASOURCE_DRIVER_CLASS_NAME", "SPRING_DATASOURCE_DRIVERCLASSNAME",
            "SPRING_JPA_DATABASE_PLATFORM", "SPRING_JPA_DATABASEPLATFORM");

    private final DataSource dataSource;
    private final Environment environment;

    ConnectionReport(DataSource dataSource, Environment environment) {
        this.dataSource = dataSource;
        this.environment = environment;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        for (String name : IGNORED_ENVIRONMENT) {
            if (System.getenv(name) != null) {
                throw new IllegalStateException(name + " is set, but the suite would ignore it;"
                        + " use -Djdbc.url, -Djdbc.driver and -Djdbc.dialect instead");
            }
        }
        try (Connection c = dataSource.getConnection()) {
            DatabaseMetaData md = c.getMetaData();
            log.info("Connected to {} with driver {} ({} {}), dialect {}", md.getURL(),
                    environment.getProperty("spring.datasource.driver-class-name"), md.getDriverName(),
                    md.getDriverVersion(), environment.getProperty("spring.jpa.database-platform"));
        }
    }
}
