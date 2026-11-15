package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The database rejects writes that break a unique, foreign-key or not-null constraint. The driver raises
 * {@link SQLIntegrityConstraintViolationException} with an SQL state of class 23, which the JDBC API specifies for
 * an integrity constraint violation, and Spring translates it to {@link DataIntegrityViolationException}.
 */
class ConstraintTest extends AcceptanceTestBase {
    @Autowired TransactionTemplate tx;

    // Each test fails on the baseline: see README.md, "Known baseline failures".
    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: sqlite-jdbc reports"
            + " SQLITE_CONSTRAINT with no SQL state, and the community dialect does not convert it")
    @Test
    void duplicateUniqueValueIsDataIntegrityViolation() {
        authors.save(new Author("One", "same@example.com"));
        assertThatThrownBy(() -> authors.saveAndFlush(new Author("Two", "same@example.com")))
                .satisfies(ConstraintTest::isIntegrityViolation);
    }

    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: the community dialect creates"
            + " no foreign keys, and sqlite-jdbc leaves foreign-key enforcement off")
    @Test
    void referenceToMissingRowIsDataIntegrityViolation() {
        // getReferenceById returns an uninitialised proxy, so Hibernate inserts the id without checking it exists.
        assertThatThrownBy(() -> tx.executeWithoutResult(
                        status -> books.saveAndFlush(new Book("Orphan", authors.getReferenceById(-1L)))))
                .satisfies(ConstraintTest::isIntegrityViolation);
        assertThat(books.count()).isZero();
    }

    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: SQLite rejects the null,"
            + " but sqlite-jdbc reports SQLITE_CONSTRAINT with no SQL state, and the community dialect does not convert it")
    @Test
    void nullInRequiredColumnIsDataIntegrityViolation() {
        Long id = authors.save(new Author("Named", "named@example.com")).getId();
        // A JPQL update goes to the database as SQL; Hibernate's own null check on persist would not.
        assertThatThrownBy(() -> authors.rename(List.of(id), null)).satisfies(ConstraintTest::isIntegrityViolation);
        assertThat(authors.findById(id).orElseThrow().getName()).isEqualTo("Named");
    }

    /** Checks Spring's translation and, separately, the driver's own exception, which Hibernate translated. */
    private static void isIntegrityViolation(Throwable thrown) {
        assertThat(thrown).isInstanceOf(DataIntegrityViolationException.class);
        Throwable t = thrown;
        while (t != null && !(t instanceof SQLException)) {
            t = t.getCause();
        }
        assertThat(t).as("the driver's SQLException behind %s", thrown)
                .isInstanceOf(SQLIntegrityConstraintViolationException.class);
        assertThat(((SQLException) t).getSQLState()).as("SQL state of %s", t).startsWith("23");
    }
}
