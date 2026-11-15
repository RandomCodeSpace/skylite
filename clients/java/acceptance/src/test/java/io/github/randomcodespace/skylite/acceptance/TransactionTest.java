package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

/** A @Transactional method spanning several repository calls commits or rolls back as one unit. */
class TransactionTest extends AcceptanceTestBase {
    @Autowired LibraryService library;

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
}
