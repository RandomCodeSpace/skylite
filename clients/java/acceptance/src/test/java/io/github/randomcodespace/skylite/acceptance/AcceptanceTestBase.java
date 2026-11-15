package io.github.randomcodespace.skylite.acceptance;

import java.sql.Connection;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** Shares one application context across test classes and empties the tables before each test. */
@SpringBootTest
@ExtendWith(Baseline.class)
abstract class AcceptanceTestBase {
    @Autowired AuthorRepository authors;
    @Autowired BookRepository books;
    @Autowired AccountRepository accounts;
    @Autowired SampleRepository samples;
    @Autowired EventRepository events;
    @Autowired DocumentRepository documents;
    @Autowired ConnectionReport connectionReport;

    @BeforeEach
    void emptyTables() {
        books.deleteAllInBatch();
        authors.deleteAllInBatch();
        accounts.deleteAllInBatch();
        samples.deleteAllInBatch();
        events.deleteAllInBatch();
        documents.deleteAllInBatch();
    }

    /** A new physical connection to the verified database, outside the application's pool and transactions. */
    Connection secondConnection() throws SQLException {
        return connectionReport.openOutsidePool();
    }

    long countFromSecondConnection(String table) throws SQLException {
        try (Connection c = secondConnection();
                var rs = c.createStatement().executeQuery("select count(*) from " + table)) {
            rs.next();
            return rs.getLong(1);
        }
    }
}
