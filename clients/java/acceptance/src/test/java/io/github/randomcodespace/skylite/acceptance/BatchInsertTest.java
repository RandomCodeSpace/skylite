package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import jakarta.persistence.EntityManagerFactory;
import java.util.List;
import java.util.stream.IntStream;
import org.hibernate.SessionFactory;
import org.hibernate.stat.Statistics;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** Several hundred inserts sent as JDBC batches. */
@SpringBootTest(properties = "spring.jpa.properties.hibernate.generate_statistics=true")
class BatchInsertTest extends AcceptanceTestBase {
    private static final int ROWS = 500;
    private static final int BATCH_SIZE = 50;

    @Autowired EntityManagerFactory emf;

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
}
