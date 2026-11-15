package io.github.randomcodespace.skylite.acceptance;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;

/** A binary column mapped with @Lob; with the community SQLite dialect, Hibernate reads it through ResultSet.getBlob. */
@Entity
public class Document {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Lob
    private byte[] content;

    protected Document() {}

    public Document(byte[] content) {
        this.content = content;
    }

    public Long getId() { return id; }
    public byte[] getContent() { return content; }
}
