package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** HikariCP hands out several connections at once, and one cannot see another's uncommitted write. */
class ConnectionPoolTest extends AcceptanceTestBase {
    @Autowired DataSource dataSource;

    @Test
    void uncommittedWriteIsInvisibleToAnotherPooledConnection() throws Exception {
        assertThat(dataSource).isInstanceOf(HikariDataSource.class);
        assertThat(((HikariDataSource) dataSource).getMaximumPoolSize()).isGreaterThan(1);

        try (Connection writer = dataSource.getConnection();
                Connection reader = dataSource.getConnection()) {
            writer.setAutoCommit(false);
            try (PreparedStatement insert = writer.prepareStatement("insert into author (name, email) values (?, ?)")) {
                insert.setString(1, "Pending");
                insert.setString(2, "pending@example.com");
                assertThat(insert.executeUpdate()).isEqualTo(1);
            }
            assertThat(count(writer)).isEqualTo(1);
            assertThat(count(reader)).isZero();
            writer.rollback();
            writer.setAutoCommit(true);
        }
        assertThat(authors.count()).isZero();
    }

    private static long count(Connection c) throws Exception {
        try (PreparedStatement q = c.prepareStatement("select count(*) from author where email = 'pending@example.com'");
                ResultSet rs = q.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
