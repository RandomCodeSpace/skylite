package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.ResultSet;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

/** Derived queries, JPQL, native queries, pagination and sorting, and update counts. */
class QueryTest extends AcceptanceTestBase {
    @Autowired DataSource dataSource;

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

    @Test
    void bulkUpdateReportsAffectedRows() {
        Long id = authors.findByEmail("bob@example.com").orElseThrow().getId();
        // The JPQL update returns the driver's update count unchanged.
        assertThat(authors.rename(id, "Robert")).isEqualTo(1);
        assertThat(authors.rename(-1L, "Nobody")).isZero();
        assertThat(authors.findById(id).orElseThrow().getName()).isEqualTo("Robert");
    }

    /**
     * Columns of two joined tables share the label {@code name}. JDBC returns the first matching column for a
     * label (java.sql.ResultSet), so {@code getString("name")} is the author's name here, not the book title.
     */
    @Test
    void duplicateColumnLabelReadsTheFirstColumn() throws Exception {
        try (Connection c = dataSource.getConnection();
                var q = c.prepareStatement("select a.name, b.title as name from author a"
                        + " join book b on b.author_id = a.id where a.email = ? order by b.title")) {
            q.setString(1, "eve@example.com");
            try (ResultSet rs = q.executeQuery()) {
                int rows = 0;
                while (rs.next()) {
                    assertThat(rs.findColumn("name")).isEqualTo(1);
                    assertThat(rs.getString("name")).isEqualTo("Eve");
                    assertThat(rs.getString(2)).isEqualTo("Eve book " + rows);
                    rows++;
                }
                assertThat(rows).isEqualTo(2);
            }
        }
    }
}
