package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.support.TransactionTemplate;

/** The database rejects writes that break a unique, foreign-key or not-null constraint, as DataIntegrityViolationException. */
class ConstraintTest extends AcceptanceTestBase {
    @Autowired TransactionTemplate tx;

    // Each test fails on the baseline: see README.md, "Known baseline failures".
    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: sqlite-jdbc reports"
            + " SQLITE_CONSTRAINT with no SQL state, and the community dialect does not convert it")
    @Test
    void duplicateUniqueValueIsDataIntegrityViolation() {
        authors.save(new Author("One", "same@example.com"));
        assertThatThrownBy(() -> authors.saveAndFlush(new Author("Two", "same@example.com")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }

    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: the community dialect creates"
            + " no foreign keys, and sqlite-jdbc leaves foreign-key enforcement off")
    @Test
    void referenceToMissingRowIsDataIntegrityViolation() {
        // getReferenceById returns an uninitialised proxy, so Hibernate inserts the id without checking it exists.
        assertThatThrownBy(() -> tx.executeWithoutResult(
                        status -> books.saveAndFlush(new Book("Orphan", authors.getReferenceById(-1L)))))
                .isInstanceOf(DataIntegrityViolationException.class);
        assertThat(books.count()).isZero();
    }

    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: SQLite rejects the null,"
            + " but sqlite-jdbc reports SQLITE_CONSTRAINT with no SQL state, and the community dialect does not convert it")
    @Test
    void nullInRequiredColumnIsDataIntegrityViolation() {
        Long id = authors.save(new Author("Named", "named@example.com")).getId();
        // A JPQL update goes to the database as SQL; Hibernate's own null check on persist would not.
        assertThatThrownBy(() -> authors.rename(id, null)).isInstanceOf(DataIntegrityViolationException.class);
        assertThat(authors.findById(id).orElseThrow().getName()).isEqualTo("Named");
    }
}
