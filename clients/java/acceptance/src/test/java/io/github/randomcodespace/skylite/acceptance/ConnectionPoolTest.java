package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * HikariCP hands out several connections at once: one cannot see another's uncommitted write, sees its commit
 * without reconnecting, and each reads back the key of its own insert.
 */
class ConnectionPoolTest extends AcceptanceTestBase {
    private static final String INSERT = "insert into author (name, email) values (?, ?)";

    @Autowired DataSource dataSource;

    @Test
    void uncommittedWriteIsInvisibleToAnotherPooledConnection() throws Exception {
        assertThat(dataSource).isInstanceOf(HikariDataSource.class);
        assertThat(((HikariDataSource) dataSource).getMaximumPoolSize()).isGreaterThan(1);

        try (Connection writer = dataSource.getConnection();
                Connection reader = dataSource.getConnection()) {
            writer.setAutoCommit(false);
            insert(writer, "pending@example.com");
            assertThat(count(writer)).isEqualTo(1);
            assertThat(count(reader)).isZero();
            writer.rollback();
            writer.setAutoCommit(true);
        }
        assertThat(authors.count()).isZero();
    }

    /**
     * The assertion runs while both connections are open and the writer is still in manual-commit mode, so only
     * the explicit commit can have published the row: closing a connection or resetting its auto-commit would
     * commit as well, and would hide a commit that does nothing. The reader reads before and after on the same
     * connection, so a reader that keeps returning what it read first is caught too.
     */
    @Test
    void commitIsVisibleToAnOpenPooledConnection() throws Exception {
        try (Connection writer = dataSource.getConnection();
                Connection reader = dataSource.getConnection()) {
            assertThat(reader.getAutoCommit()).isTrue();
            assertThat(count(reader)).isZero();
            writer.setAutoCommit(false);
            try {
                insert(writer, "pending@example.com");
                writer.commit();
                assertThat(count(reader)).isEqualTo(1);
            } finally {
                writer.setAutoCommit(true);
            }
        }
    }

    /** Two connections insert one after the other; each statement's generated key is the id of its own row. */
    @Test
    void generatedKeyBelongsToTheStatementsOwnInsert() throws Exception {
        try (Connection first = dataSource.getConnection();
                Connection second = dataSource.getConnection();
                PreparedStatement insertFirst = first.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS);
                PreparedStatement insertSecond = second.prepareStatement(INSERT, Statement.RETURN_GENERATED_KEYS)) {
            bind(insertFirst, "first@example.com");
            assertThat(insertFirst.executeUpdate()).isEqualTo(1);
            bind(insertSecond, "second@example.com");
            assertThat(insertSecond.executeUpdate()).isEqualTo(1);

            long firstId = generatedKey(insertFirst);
            long secondId = generatedKey(insertSecond);
            assertThat(firstId).isNotEqualTo(secondId);
            assertThat(authors.findById(firstId).orElseThrow().getEmail()).isEqualTo("first@example.com");
            assertThat(authors.findById(secondId).orElseThrow().getEmail()).isEqualTo("second@example.com");
        }
    }

    private static void insert(Connection c, String email) throws Exception {
        try (PreparedStatement insert = c.prepareStatement(INSERT)) {
            bind(insert, email);
            assertThat(insert.executeUpdate()).isEqualTo(1);
        }
    }

    private static void bind(PreparedStatement insert, String email) throws Exception {
        insert.setString(1, "Pending");
        insert.setString(2, email);
    }

    private static long generatedKey(PreparedStatement insert) throws Exception {
        try (ResultSet keys = insert.getGeneratedKeys()) {
            assertThat(keys.next()).isTrue();
            return keys.getLong(1);
        }
    }

    private static long count(Connection c) throws Exception {
        try (PreparedStatement q = c.prepareStatement("select count(*) from author where email = 'pending@example.com'");
                ResultSet rs = q.executeQuery()) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
