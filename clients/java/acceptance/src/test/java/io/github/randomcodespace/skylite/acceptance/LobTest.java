package io.github.randomcodespace.skylite.acceptance;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Random;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;

/** A @Lob byte[] of a few hundred KiB round-trips. */
class LobTest extends AcceptanceTestBase {
    // Fails on the baseline: see README.md, "Known baseline failures".
    @DisabledIf(value = Baseline.CONDITION, disabledReason = "known baseline failure: with the community dialect,"
            + " Hibernate reads @Lob byte[] with ResultSet.getBlob, which sqlite-jdbc does not implement")
    @Test
    void lobRoundTrips() {
        byte[] content = new byte[300 * 1024];
        new Random(7).nextBytes(content);
        Long id = documents.save(new Document(content)).getId();
        assertThat(documents.findById(id).orElseThrow().getContent()).isEqualTo(content);
    }
}
