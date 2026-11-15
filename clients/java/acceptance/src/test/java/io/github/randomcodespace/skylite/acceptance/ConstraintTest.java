package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;

/** A unique-constraint violation surfaces as Spring's DataIntegrityViolationException. */
class ConstraintTest extends AcceptanceTestBase {
    // Fails on the baseline: see README.md, "Known baseline failures".
    @Disabled("Hibernate SQLiteDialect does not map SQLITE_CONSTRAINT to ConstraintViolationException")
    @Test
    void duplicateUniqueValueIsDataIntegrityViolation() {
        authors.save(new Author("One", "same@example.com"));
        assertThatThrownBy(() -> authors.saveAndFlush(new Author("Two", "same@example.com")))
                .isInstanceOf(DataIntegrityViolationException.class);
    }
}
