package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * A @Transactional method spanning several repository calls commits or rolls back as one unit; a savepoint
 * rolls back only what followed it; a failed statement leaves the transaction usable.
 */
class TransactionTest extends AcceptanceTestBase {
    private static final String INSERT = "insert into author (name, email) values ('Writer', ?)";

    @Autowired LibraryService library;
    @Autowired DataSource dataSource;

    @Test
    void commitIsVisibleFromSecondConnection() throws Exception {
        library.createAuthorWithBooks("Iain", "iain@example.com", List.of("Excession", "Use of Weapons"));
        assertThat(countFromSecondConnection("author")).isEqualTo(1);
        assertThat(countFromSecondConnection("book")).isEqualTo(2);
    }

    @Test
    void exceptionRollsBackEveryWrite() throws Exception {
        assertThatThrownBy(() -> library.createAuthorWithBooksThenFail(
                        "Octavia", "octavia@example.com", List.of("Kindred", "Dawn")))
                .isInstanceOf(IllegalStateException.class);
        assertThat(countFromSecondConnection("author")).isZero();
        assertThat(countFromSecondConnection("book")).isZero();
        assertThat(authors.findByEmail("octavia@example.com")).isEmpty();
    }

    // Skipped where the driver reports no savepoints: see README.md, "Running against skylite".
    @DisabledIf(value = Baseline.NO_SAVEPOINTS, disabledReason = "the driver reports no savepoint support; skylite's"
            + " server refuses SAVEPOINT, RELEASE and ROLLBACK TO on a write channel")
    @Test
    void rollbackToSavepointKeepsEarlierWrites() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                insert(c, "before@example.com");
                Savepoint savepoint = c.setSavepoint();
                insert(c, "after@example.com");
                c.rollback(savepoint);
                c.commit();
            } finally {
                c.setAutoCommit(true);
            }
        }
        assertThat(countFromSecondConnection("author")).isEqualTo(1);
        assertThat(authors.findByEmail("before@example.com")).isPresent();
    }

    /** A statement that fails on a constraint is undone on its own; the transaction's other writes commit. */
    @Test
    void failedStatementLeavesTransactionUsable() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try {
                insert(c, "first@example.com");
                assertThatThrownBy(() -> insert(c, "first@example.com")).isInstanceOf(SQLException.class);
                insert(c, "second@example.com");
                c.commit();
            } finally {
                c.setAutoCommit(true);
            }
        }
        assertThat(countFromSecondConnection("author")).isEqualTo(2);
    }

    private static void insert(Connection c, String email) throws SQLException {
        try (var insert = c.prepareStatement(INSERT)) {
            insert.setString(1, email);
            insert.executeUpdate();
        }
    }
}
