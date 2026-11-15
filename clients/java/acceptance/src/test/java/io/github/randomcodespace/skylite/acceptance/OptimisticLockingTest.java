package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.orm.ObjectOptimisticLockingFailureException;

/** @Version increments on update and a stale update is rejected. */
class OptimisticLockingTest extends AcceptanceTestBase {
    @Test
    void versionIncrementsAndStaleUpdateFails() {
        Long id = accounts.save(new Account(100)).getId();

        Account first = accounts.findById(id).orElseThrow();
        Account stale = accounts.findById(id).orElseThrow();
        assertThat(first.getVersion()).isEqualTo(stale.getVersion());

        first.setBalance(150);
        Account saved = accounts.save(first);
        assertThat(saved.getVersion()).isGreaterThan(stale.getVersion());

        stale.setBalance(50);
        assertThatThrownBy(() -> accounts.save(stale)).isInstanceOf(ObjectOptimisticLockingFailureException.class);
        assertThat(accounts.findById(id).orElseThrow().getBalance()).isEqualTo(150);
    }
}
