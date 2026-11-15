package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import java.util.List;
import org.hibernate.Hibernate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.support.TransactionTemplate;

/** One-to-many and many-to-one associations: lazy loading, join fetch, cascade and orphan removal. */
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
            assertThat(loaded.getBooks()).extracting(Book::getTitle)
                    .containsExactlyInAnyOrder("The Dispossessed", "A Wizard of Earthsea");
        });

        assertThat(library.bookTitles(authorId)).isEqualTo(List.of("A Wizard of Earthsea", "The Dispossessed"));
    }

    @Test
    void joinFetchLoadsParentAndChildren() {
        Author author = new Author("Iris", "iris@example.com");
        author.addBook("Under the Net");
        author.addBook("The Sea, the Sea");
        Author saved = authors.save(author);
        Long authorId = saved.getId();

        // Outside a transaction, so a fresh persistence context; each result row holds author and book columns.
        Author loaded = authors.findWithBooksByEmail("iris@example.com").orElseThrow();
        assertThat(loaded.getId()).isEqualTo(authorId);
        assertThat(loaded.getName()).isEqualTo("Iris");
        assertThat(Hibernate.isInitialized(loaded.getBooks())).isTrue();
        assertThat(loaded.getBooks()).extracting(Book::getId, Book::getTitle).containsExactlyInAnyOrderElementsOf(
                saved.getBooks().stream().map(b -> tuple(b.getId(), b.getTitle())).toList());
        assertThat(loaded.getBooks()).extracting(Book::getTitle)
                .containsExactlyInAnyOrder("Under the Net", "The Sea, the Sea");
        assertThat(loaded.getBooks()).allSatisfy(b -> assertThat(b.getAuthor()).isSameAs(loaded));
    }

    /** Author.books cascades every operation and removes orphans. */
    @Test
    void cascadeAndOrphanRemovalDeleteBooks() throws Exception {
        Author author = new Author("Ted", "ted@example.com");
        author.addBook("Stories of Your Life");
        author.addBook("Exhalation");
        Long authorId = authors.save(author).getId();
        assertThat(countFromSecondConnection("book")).isEqualTo(2);

        tx.executeWithoutResult(status -> authors.findById(authorId).orElseThrow().getBooks()
                .removeIf(b -> b.getTitle().equals("Exhalation")));
        assertThat(countFromSecondConnection("book")).isEqualTo(1);
        assertThat(library.bookTitles(authorId)).containsExactly("Stories of Your Life");

        authors.deleteById(authorId);
        assertThat(countFromSecondConnection("author")).isZero();
        assertThat(countFromSecondConnection("book")).isZero();
    }
}
