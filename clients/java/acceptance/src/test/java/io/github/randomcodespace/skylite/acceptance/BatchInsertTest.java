package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.tuple;

import jakarta.persistence.EntityManagerFactory;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.support.TransactionTemplate;

/** Several hundred inserts and deletes sent as JDBC batches, and a batch that fails inside a transaction. */
@SpringBootTest(properties = "spring.jpa.properties.hibernate.generate_statistics=true")
class BatchInsertTest extends AcceptanceTestBase {
    private static final int ROWS = 500;
    private static final int BATCH_SIZE = 50;

    @Autowired EntityManagerFactory emf;
    @Autowired DataSource dataSource;
    @Autowired TransactionTemplate tx;

    @Test
    void insertsAreBatched() {
        SessionFactory sf = emf.unwrap(SessionFactory.class);
        assertThat(sf.getSessionFactoryOptions().getJdbcBatchSize()).isEqualTo(BATCH_SIZE);
        Statistics stats = sf.getStatistics();
        stats.clear();

        List<Event> saved = events.saveAll(IntStream.range(0, ROWS).mapToObj(Event::new).toList());

        assertThat(stats.getEntityInsertCount()).isEqualTo(ROWS);
        // Unbatched, Hibernate prepares the insert once per row; batched, it reuses one statement across batches.
        assertThat(stats.getPrepareStatementCount()).isLessThanOrEqualTo(ROWS / BATCH_SIZE);
        assertThat(events.count()).isEqualTo(ROWS);

        // Read back outside the saving transaction, so through a fresh persistence context: every (id, seq) pair
        // bound in the batch arrives, once, unchanged, and in the order the rows were added to the batch.
        assertThat(events.findAll()).extracting(Event::getId, Event::getSeq)
                .containsExactlyInAnyOrderElementsOf(saved.stream().map(e -> tuple(e.getId(), e.getSeq())).toList());
        assertThat(events.seqInInsertionOrder()).isEqualTo(IntStream.range(0, ROWS).boxed().toList());
    }

    @Test
    void deletesAreBatched() {
        SessionFactory sf = emf.unwrap(SessionFactory.class);
        events.saveAll(IntStream.range(0, ROWS).mapToObj(Event::new).toList());
        Statistics stats = sf.getStatistics();

        // Loaded in the deleting transaction, so that Spring Data's find before each delete selects nothing.
        tx.executeWithoutResult(status -> {
            List<Event> loaded = events.findAll();
            stats.clear();
            events.deleteAll(loaded.stream().filter(e -> e.getSeq() != ROWS - 1).toList());
        });

        assertThat(stats.getEntityDeleteCount()).isEqualTo(ROWS - 1);
        assertThat(stats.getPrepareStatementCount()).isLessThanOrEqualTo(ROWS / BATCH_SIZE + 1);
        assertThat(events.findAll()).extracting(Event::getSeq).containsExactly(ROWS - 1);
    }

    /**
     * A batch that fails inside a transaction stops at the failing entry, which the exception reports through the
     * update counts of the entries before it; rolling back undoes those too.
     */
    @Test
    void aFailedBatchInATransactionRollsBack() throws Exception {
        try (Connection c = dataSource.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement insert = c.prepareStatement("insert into author (name, email) values (?, ?)")) {
                for (String email : List.of("a@example.com", "b@example.com", "a@example.com", "c@example.com")) {
                    insert.setString(1, "Writer");
                    insert.setString(2, email);
                    insert.addBatch();
                }
                boolean skylite = c.getMetaData().getDriverName().startsWith("skylite");
                // sqlite-jdbc raises the entry's own exception, not a BatchUpdateException, so only skylite's is checked
                assertThatThrownBy(insert::executeBatch).isInstanceOfSatisfying(SQLException.class, e -> {
                    if (skylite) {
                        assertThat((Object) e).isInstanceOf(BatchUpdateException.class);
                        assertThat(((BatchUpdateException) e).getUpdateCounts()).containsExactly(1, 1);
                    }
                });
            } finally {
                c.rollback();
                c.setAutoCommit(true);
            }
        }
        assertThat(countFromSecondConnection("author")).isZero();
    }
}
