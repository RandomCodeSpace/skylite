package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

/** Derived queries, JPQL, a native query, pagination and sorting. */
class QueryTest extends AcceptanceTestBase {
    @BeforeEach
    void seed() {
        String[] names = {"Carol", "Alice", "Eve", "Bob", "Dave", "Alan", "Frank"};
        for (int i = 0; i < names.length; i++) {
            Author a = new Author(names[i], names[i].toLowerCase() + "@example.com");
            for (int b = 0; b < i % 3; b++) {
                a.addBook(names[i] + " book " + b);
            }
            authors.save(a);
        }
    }

    @Test
    void derivedQuery() {
        assertThat(authors.findByNameStartingWithOrderByNameAsc("Al")).extracting(Author::getName)
                .containsExactly("Alan", "Alice");
        assertThat(authors.findByEmail("eve@example.com")).isPresent();
    }

    @Test
    void jpqlQuery() {
        // Books per author by index: Carol 0, Alice 1, Eve 2, Bob 0, Dave 1, Alan 2, Frank 0.
        assertThat(authors.findWithAtLeastBooks(2)).extracting(Author::getName).containsExactly("Alan", "Eve");
    }

    @Test
    void nativeQuery() {
        assertThat(authors.countByNameLikeNative("a%")).isEqualTo(2);
    }

    @Test
    void paginationAndSorting() {
        Page<Author> page = authors.findAll(PageRequest.of(1, 3, Sort.by("name").descending())); // Frank Eve Dave | Carol Bob Alice | Alan
        assertThat(page.getTotalElements()).isEqualTo(7);
        assertThat(page.getTotalPages()).isEqualTo(3);
        assertThat(page.getContent()).extracting(Author::getName).containsExactly("Carol", "Bob", "Alice");
    }
}
