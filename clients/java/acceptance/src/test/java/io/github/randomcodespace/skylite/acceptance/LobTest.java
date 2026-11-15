package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/** A @Lob byte[] of a few hundred KiB round-trips, and so do an empty one and null. */
class LobTest extends AcceptanceTestBase {
    // Fails on the baseline: see README.md, "Known baseline failures".
    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: with the community dialect,"
            + " Hibernate reads @Lob byte[] with ResultSet.getBlob, which sqlite-jdbc does not implement")
    @Test
    void lobRoundTrips() {
        byte[] content = new byte[300 * 1024];
        new Random(7).nextBytes(content);
        // A copy taken before saving: the driver receives the entity's own array and could change it.
        byte[] expected = content.clone();
        Long id = documents.save(new Document(content)).getId();
        Long emptyId = documents.save(new Document(new byte[0])).getId();
        Long nullId = documents.save(new Document(null)).getId();
        assertThat(documents.findById(id).orElseThrow().getContent()).isEqualTo(expected);
        assertThat(documents.findById(emptyId).orElseThrow().getContent()).isNotNull().isEmpty();
        assertThat(documents.findById(nullId).orElseThrow().getContent()).isNull();
    }
}
