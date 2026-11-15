package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.hibernate.Hibernate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/** One-to-many and many-to-one associations, loaded lazily inside a transaction. */
class RelationsTest extends AcceptanceTestBase {
    @Autowired LibraryService library;
    @Autowired TransactionTemplate tx;

    @Test
    void lazyAssociationsLoadInsideTransaction() {
        Author author = new Author("Ursula", "ursula@example.com");
        author.addBook("The Dispossessed");
        author.addBook("A Wizard of Earthsea");
        Long authorId = authors.save(author).getId();

        assertThat(library.bookTitles(authorId)).containsExactly("A Wizard of Earthsea", "The Dispossessed");

        Long bookId = books.findAll().getFirst().getId();
        assertThat(library.authorNameOfBook(bookId)).isEqualTo("Ursula");

        tx.executeWithoutResult(status -> {
            Book book = books.findById(bookId).orElseThrow();
            assertThat(Hibernate.isInitialized(book.getAuthor())).isFalse();
            assertThat(book.getAuthor().getEmail()).isEqualTo("ursula@example.com");
            Author loaded = authors.findById(authorId).orElseThrow();
            assertThat(Hibernate.isInitialized(loaded.getBooks())).isFalse();
            assertThat(loaded.getBooks()).hasSize(2);
        });

        assertThat(library.bookTitles(authorId)).isEqualTo(List.of("A Wizard of Earthsea", "The Dispossessed"));
    }
}
