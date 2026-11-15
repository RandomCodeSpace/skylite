package io.github.randomcodespace.skylite.acceptance;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import java.util.UUID;

/** Batch-insert entity. Identifiers are generated in the application because Hibernate does not batch IDENTITY inserts. */
@Entity
public class Event {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    private int seq;

    protected Event() {}

    public Event(int seq) {
        this.seq = seq;
    }

    public UUID getId() { return id; }
    public int getSeq() { return seq; }
}
