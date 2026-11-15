package io.github.randomcodespace.skylite.acceptance;

import java.util.List;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** Service methods whose transactions span several repository calls. */
@Service
public class LibraryService {
    private final AuthorRepository authors;
    private final BookRepository books;

    public LibraryService(AuthorRepository authors, BookRepository books) {
        this.authors = authors;
        this.books = books;
    }

    /** Saves an author, then each book in a separate repository call, in one transaction. */
    @Transactional
    public Long createAuthorWithBooks(String name, String email, List<String> titles) {
        Author author = authors.save(new Author(name, email));
        for (String title : titles) {
            books.save(new Book(title, author));
        }
        return author.getId();
    }

    /** Same writes as {@link #createAuthorWithBooks}, then fails, so the transaction must roll back. */
    @Transactional
    public void createAuthorWithBooksThenFail(String name, String email, List<String> titles) {
        createAuthorWithBooks(name, email, titles);
        authors.flush();
        throw new IllegalStateException("fail after writing");
    }

    /** Navigates a lazy one-to-many collection inside a read-only transaction. */
    @Transactional(readOnly = true)
    public List<String> bookTitles(Long authorId) {
        return authors.findById(authorId).orElseThrow().getBooks().stream().map(Book::getTitle).sorted().toList();
    }

    /** Navigates a lazy many-to-one association inside a read-only transaction. */
    @Transactional(readOnly = true)
    public String authorNameOfBook(Long bookId) {
        return books.findById(bookId).orElseThrow().getAuthor().getName();
    }
}
