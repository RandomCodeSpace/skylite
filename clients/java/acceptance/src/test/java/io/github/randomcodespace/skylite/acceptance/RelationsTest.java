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

    @Test
    void joinFetchLoadsParentAndChildren() {
        Author author = new Author("Iris", "iris@example.com");
        author.addBook("Under the Net");
        author.addBook("The Sea, the Sea");
        Long authorId = authors.save(author).getId();

        // Outside a transaction, so a fresh persistence context; each result row holds author and book columns.
        Author loaded = authors.findWithBooksByEmail("iris@example.com").orElseThrow();
        assertThat(loaded.getId()).isEqualTo(authorId);
        assertThat(loaded.getName()).isEqualTo("Iris");
        assertThat(Hibernate.isInitialized(loaded.getBooks())).isTrue();
        assertThat(loaded.getBooks()).extracting(Book::getTitle)
                .containsExactlyInAnyOrder("Under the Net", "The Sea, the Sea");
        assertThat(loaded.getBooks()).extracting(Book::getId).doesNotContainNull().doesNotHaveDuplicates();
        assertThat(loaded.getBooks()).allSatisfy(b -> assertThat(b.getAuthor()).isSameAs(loaded));
    }
}
