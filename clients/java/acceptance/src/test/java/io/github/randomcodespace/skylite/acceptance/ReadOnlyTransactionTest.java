package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.persistence.EntityManager;
import java.sql.Connection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** A Spring read-only transaction reads one snapshot and refuses writes. */
class ReadOnlyTransactionTest extends AcceptanceTestBase {
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired EntityManager em;

    private TransactionTemplate readOnly() {
        TransactionTemplate t = new TransactionTemplate(transactionManager);
        t.setReadOnly(true);
        return t;
    }

    /** Another connection's commit during the transaction is not seen by it, and is seen after it. */
    @Test
    void readsOneSnapshot() {
        authors.save(new Author("First", "first@example.com"));
        long[] counts = readOnly().execute(status -> {
            long before = authors.count();
            try (Connection c = secondConnection(); var insert = c.createStatement()) {
                insert.executeUpdate("insert into author (name, email) values ('Second', 'second@example.com')");
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            return new long[] {before, authors.count()};
        });
        assertThat(counts).containsExactly(1, 1);
        assertThat(authors.count()).isEqualTo(2);
    }

    // Fails on the baseline: see README.md, "Known baseline failures".
    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: sqlite-jdbc cannot make an open"
            + " connection read-only, so Spring's read-only transaction writes")
    @Test
    void refusesWrites() {
        assertThatThrownBy(() -> readOnly().executeWithoutResult(status -> em.createNativeQuery(
                        "insert into author (name, email) values ('Writer', 'writer@example.com')").executeUpdate()))
                .isInstanceOf(RuntimeException.class);
        assertThat(authors.count()).isZero();
    }
}
