package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** DatabaseMetaData names the database engine and version the connection actually runs on. */
class MetadataTest extends AcceptanceTestBase {
    @Autowired DataSource dataSource;

    @Test
    void productNameAndVersionMatchTheEngine() throws Exception {
        try (Connection c = dataSource.getConnection();
                ResultSet rs = c.createStatement().executeQuery("select sqlite_version()")) {
            assertThat(rs.next()).isTrue();
            String engineVersion = rs.getString(1);
            String[] parts = engineVersion.split("\\.");
            DatabaseMetaData md = c.getMetaData();
            assertThat(md.getDatabaseProductName()).isEqualTo("SQLite");
            assertThat(md.getDatabaseProductVersion()).isEqualTo(engineVersion);
            assertThat(md.getDatabaseMajorVersion()).isEqualTo(Integer.parseInt(parts[0]));
            assertThat(md.getDatabaseMinorVersion()).isEqualTo(Integer.parseInt(parts[1]));
        }
    }
}
