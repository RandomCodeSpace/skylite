package io.github.randomcodespace.skylite.acceptance;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface EventRepository extends JpaRepository<Event, UUID> {
    /** SQLite assigns each new row the next rowid, so rowid order is the order the inserts were executed in. */
    @Query(value = "select seq from event order by rowid", nativeQuery = true)
    List<Integer> seqInInsertionOrder();
}
