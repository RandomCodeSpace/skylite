package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.persistence.EntityManager;
import org.hibernate.Session;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.transaction.support.TransactionTemplate;

/** @Version increments on update, and a stale update is rejected. */
class OptimisticLockingTest extends AcceptanceTestBase {
    @Autowired TransactionTemplate tx;
    @Autowired EntityManager em;

    /** Detached copies: Hibernate's merge loads the current row and rejects the version before any UPDATE. */
    @Test
    void versionIncrementsAndStaleUpdateFails() {
        Long id = accounts.save(new Account(100)).getId();

        Account first = accounts.findById(id).orElseThrow();
        Account stale = accounts.findById(id).orElseThrow();
        assertThat(first.getVersion()).isEqualTo(stale.getVersion());

        first.setBalance(150);
        Account saved = accounts.save(first);
        assertThat(saved.getVersion()).isEqualTo(stale.getVersion() + 1);

        stale.setBalance(50);
        assertThatThrownBy(() -> accounts.save(stale)).isInstanceOf(ObjectOptimisticLockingFailureException.class);
        assertThat(accounts.findById(id).orElseThrow().getBalance()).isEqualTo(150);
    }

    /**
     * A managed entity whose row changed underneath it: the flush sends {@code update ... where id = ? and
     * version = ?}, which matches no row, and only the driver's update count of 0 tells Hibernate so.
     */
    @Test
    void updateMatchingNoRowFailsOnFlush() {
        Long id = accounts.save(new Account(100)).getId();

        assertThatThrownBy(() -> tx.executeWithoutResult(status -> {
                    Account managed = accounts.findById(id).orElseThrow();
                    // Advance the stored version on the transaction's own connection, without refreshing the entity.
                    em.unwrap(Session.class).doWork(c -> {
                        try (var update = c.prepareStatement("update account set version = version + 1 where id = ?")) {
                            update.setLong(1, id);
                            update.executeUpdate();
                        }
                    });
                    managed.setBalance(50);
                    accounts.flush();
                }))
                .isInstanceOf(ObjectOptimisticLockingFailureException.class);
        assertThat(accounts.findById(id).orElseThrow().getBalance()).isEqualTo(100);
    }
}
